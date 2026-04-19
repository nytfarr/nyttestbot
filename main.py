"""
Telegram Refer-to-Earn Bot
Library: pyTelegramBotAPI
Database: PostgreSQL (Supabase via psycopg2)
"""

import os
import logging
import threading
import time
from datetime import datetime
from functools import wraps

import psycopg2
import psycopg2.extras
from psycopg2 import pool

import telebot
from telebot import types

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────

BOT_TOKEN      = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
OWNER_ID       = int(os.getenv("OWNER_ID", "8499435987"))
ADMIN_USERNAME = "@SoulFoex"
BOT_USERNAME   = os.getenv("BOT_USERNAME", "YourBotUsername")

REFERRAL_REWARD   = 20
MIN_WITHDRAW      = 100
ACTIVATION_COST   = 50
ACTIVATION_NUMBER = "01705930972"

DATABASE_URL = os.getenv("DATABASE_URL", "")

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  BOT INIT
# ─────────────────────────────────────────────

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ─────────────────────────────────────────────
#  DATABASE LAYER
# ─────────────────────────────────────────────

_pool: pool.ThreadedConnectionPool = None
_pool_lock = threading.Lock()


def get_pool() -> pool.ThreadedConnectionPool:
    global _pool
    if _pool is None or _pool.closed:
        with _pool_lock:
            if _pool is None or _pool.closed:
                # Ensure sslmode=require for Supabase
                dsn = DATABASE_URL
                if "sslmode" not in dsn:
                    sep = "&" if "?" in dsn else "?"
                    dsn = dsn + sep + "sslmode=require"

                _pool = pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=10,
                    dsn=dsn,
                    cursor_factory=psycopg2.extras.RealDictCursor,
                    connect_timeout=10,
                )
                logger.info("PostgreSQL connection pool created.")
    return _pool


def get_conn():
    return get_pool().getconn()


def release_conn(conn):
    try:
        get_pool().putconn(conn)
    except Exception as exc:
        logger.warning("Failed to release connection: %s", exc)


class _DBConn:
    def __enter__(self):
        self.conn = get_conn()
        self.conn.autocommit = False
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            try:
                self.conn.rollback()
            except Exception:
                pass
            logger.error("DB rollback due to: %s", exc_val)
        else:
            self.conn.commit()
        release_conn(self.conn)
        return False


def init_db(retries=10, delay=5):
    """Try to connect and create tables, retrying on failure."""
    for attempt in range(1, retries + 1):
        try:
            with _DBConn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS users (
                            user_id       BIGINT PRIMARY KEY,
                            username      TEXT DEFAULT '',
                            first_name    TEXT DEFAULT '',
                            balance       INTEGER DEFAULT 0,
                            referrals     INTEGER DEFAULT 0,
                            referred_by   BIGINT DEFAULT NULL,
                            is_activated  INTEGER DEFAULT 0,
                            is_banned     INTEGER DEFAULT 0,
                            joined_at     TIMESTAMPTZ DEFAULT NOW()
                        );
                    """)
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS withdrawals (
                            id            SERIAL PRIMARY KEY,
                            user_id       BIGINT NOT NULL,
                            amount        INTEGER NOT NULL,
                            method        TEXT NOT NULL,
                            number        TEXT NOT NULL,
                            status        TEXT DEFAULT 'pending',
                            created_at    TIMESTAMPTZ DEFAULT NOW(),
                            resolved_at   TIMESTAMPTZ DEFAULT NULL
                        );
                    """)
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS activations (
                            id            SERIAL PRIMARY KEY,
                            user_id       BIGINT NOT NULL,
                            status        TEXT DEFAULT 'pending',
                            created_at    TIMESTAMPTZ DEFAULT NOW(),
                            resolved_at   TIMESTAMPTZ DEFAULT NULL
                        );
                    """)
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS user_states (
                            user_id       BIGINT PRIMARY KEY,
                            state         TEXT DEFAULT NULL,
                            data          TEXT DEFAULT NULL
                        );
                    """)
                    cur.execute("""
                        DO $$
                        BEGIN
                            IF NOT EXISTS (
                                SELECT 1 FROM information_schema.columns
                                WHERE table_name = 'users' AND column_name = 'is_banned'
                            ) THEN
                                ALTER TABLE users ADD COLUMN is_banned INTEGER DEFAULT 0;
                            END IF;
                        END$$;
                    """)
            logger.info("Database initialised successfully.")
            return
        except Exception as e:
            logger.error("DB init attempt %d/%d failed: %s", attempt, retries, e)
            # Reset pool so next attempt creates a fresh connection
            global _pool
            _pool = None
            if attempt < retries:
                logger.info("Retrying in %d seconds...", delay)
                time.sleep(delay)
            else:
                raise RuntimeError("Could not connect to database after multiple attempts.") from e


# ── User helpers ──────────────────────────────

def db_get_user(user_id: int):
    with _DBConn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
            return cur.fetchone()


def db_get_user_by_username(username: str):
    username = username.lstrip("@")
    with _DBConn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM users WHERE LOWER(username) = LOWER(%s)",
                (username,),
            )
            return cur.fetchone()


def db_create_user(user_id: int, username: str, first_name: str, referred_by: int = None):
    with _DBConn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (user_id, username, first_name, referred_by)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id) DO NOTHING
                """,
                (user_id, username or "", first_name or "", referred_by),
            )
            if referred_by:
                cur.execute(
                    """
                    UPDATE users
                    SET balance = balance + %s,
                        referrals = referrals + 1
                    WHERE user_id = %s
                    """,
                    (REFERRAL_REWARD, referred_by),
                )


def db_update_user(user_id: int, **kwargs):
    if not kwargs:
        return
    fields = ", ".join(f"{k} = %s" for k in kwargs)
    values = list(kwargs.values()) + [user_id]
    with _DBConn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE users SET {fields} WHERE user_id = %s", values)


def db_get_top_referrers(limit: int = 10):
    with _DBConn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT user_id, first_name, username, referrals FROM users ORDER BY referrals DESC LIMIT %s",
                (limit,),
            )
            return cur.fetchall()


# ── Withdrawal helpers ────────────────────────

def db_create_withdrawal(user_id: int, amount: int, method: str, number: str) -> int:
    with _DBConn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO withdrawals (user_id, amount, method, number) VALUES (%s, %s, %s, %s) RETURNING id",
                (user_id, amount, method, number),
            )
            return cur.fetchone()["id"]


def db_get_withdrawal(w_id: int):
    with _DBConn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM withdrawals WHERE id = %s", (w_id,))
            return cur.fetchone()


def db_update_withdrawal(w_id: int, status: str):
    with _DBConn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE withdrawals SET status = %s, resolved_at = NOW() WHERE id = %s",
                (status, w_id),
            )


def db_pending_withdrawal_exists(user_id: int) -> bool:
    with _DBConn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM withdrawals WHERE user_id = %s AND status = 'pending' LIMIT 1",
                (user_id,),
            )
            return cur.fetchone() is not None


# ── Activation helpers ────────────────────────

def db_create_activation(user_id: int) -> int:
    with _DBConn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO activations (user_id) VALUES (%s) RETURNING id",
                (user_id,),
            )
            return cur.fetchone()["id"]


def db_get_activation(a_id: int):
    with _DBConn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM activations WHERE id = %s", (a_id,))
            return cur.fetchone()


def db_update_activation(a_id: int, status: str):
    with _DBConn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE activations SET status = %s, resolved_at = NOW() WHERE id = %s",
                (status, a_id),
            )


def db_pending_activation_exists(user_id: int) -> bool:
    with _DBConn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM activations WHERE user_id = %s AND status = 'pending' LIMIT 1",
                (user_id,),
            )
            return cur.fetchone() is not None


# ── State helpers ─────────────────────────────

def set_state(user_id: int, state: str, data: str = ""):
    with _DBConn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_states (user_id, state, data)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE
                    SET state = EXCLUDED.state, data = EXCLUDED.data
                """,
                (user_id, state, data),
            )


def get_state(user_id: int):
    with _DBConn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT state, data FROM user_states WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
            return (row["state"], row["data"]) if row else (None, None)


def clear_state(user_id: int):
    with _DBConn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM user_states WHERE user_id = %s", (user_id,))


# ── Count helpers ─────────────────────────────

def db_count_withdrawals(user_id: int) -> int:
    with _DBConn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS cnt FROM withdrawals WHERE user_id = %s AND status = 'approved'",
                (user_id,),
            )
            row = cur.fetchone()
            return row["cnt"] if row else 0


def db_count_pending_activations(user_id: int) -> int:
    with _DBConn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS cnt FROM activations WHERE user_id = %s AND status = 'pending'",
                (user_id,),
            )
            row = cur.fetchone()
            return row["cnt"] if row else 0


# ─────────────────────────────────────────────
#  KEYBOARDS
# ─────────────────────────────────────────────

def kb_main() -> types.ReplyKeyboardMarkup:
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(
        types.KeyboardButton("💰 Refer To Earn"),
        types.KeyboardButton("👤 Admin"),
        types.KeyboardButton("💳 Withdraw"),
        types.KeyboardButton("📊 Profile"),
    )
    return kb


def kb_back() -> types.ReplyKeyboardMarkup:
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(types.KeyboardButton("🔙 Back"))
    return kb


def kb_withdraw_method() -> types.ReplyKeyboardMarkup:
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(
        types.KeyboardButton("📱 Bkash"),
        types.KeyboardButton("📲 Nagad"),
        types.KeyboardButton("🔙 Back"),
    )
    return kb


def kb_activation() -> types.ReplyKeyboardMarkup:
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(
        types.KeyboardButton("✅ Done"),
        types.KeyboardButton("❌ Cancel"),
    )
    return kb


def kb_inline_approve_reject(action: str, record_id: int) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("✅ Approve", callback_data=f"{action}_approve_{record_id}"),
        types.InlineKeyboardButton("❌ Reject", callback_data=f"{action}_reject_{record_id}"),
    )
    return kb


# ─────────────────────────────────────────────
#  DECORATORS
# ─────────────────────────────────────────────

def owner_only(func):
    @wraps(func)
    def wrapper(call_or_msg, *args, **kwargs):
        uid = (
            call_or_msg.from_user.id
            if hasattr(call_or_msg, "from_user")
            else call_or_msg.message.from_user.id
        )
        if uid != OWNER_ID:
            if hasattr(call_or_msg, "id"):
                bot.answer_callback_query(call_or_msg.id, "⛔ Unauthorised.")
            return
        return func(call_or_msg, *args, **kwargs)
    return wrapper


def ensure_registered(func):
    @wraps(func)
    def wrapper(msg, *args, **kwargs):
        user = msg.from_user
        if not db_get_user(user.id):
            db_create_user(user.id, user.username, user.first_name)
        return func(msg, *args, **kwargs)
    return wrapper


def check_banned(func):
    @wraps(func)
    def wrapper(msg, *args, **kwargs):
        user = db_get_user(msg.from_user.id)
        if user and user["is_banned"]:
            bot.send_message(msg.chat.id, "⛔ You have been banned by admin.")
            return
        return func(msg, *args, **kwargs)
    return wrapper


# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────

def referral_link(user_id: int) -> str:
    return f"[t.me](https://t.me/{BOT_USERNAME}?start=ref_{user_id})"


def fmt_name(row: dict) -> str:
    name = row["first_name"] or "User"
    return f"@{row['username']}" if row["username"] else name


def safe_send(user_id: int, text: str, **kwargs):
    try:
        bot.send_message(user_id, text, **kwargs)
    except Exception as exc:
        logger.warning("Could not send to %s: %s", user_id, exc)


# ─────────────────────────────────────────────
#  /start
# ─────────────────────────────────────────────

@bot.message_handler(commands=["start"])
def cmd_start(msg: types.Message):
    user  = msg.from_user
    args  = msg.text.split(maxsplit=1)
    param = args[1] if len(args) > 1 else ""
    ref_id = None

    if param.startswith("ref_"):
        try:
            ref_id = int(param[4:])
        except ValueError:
            ref_id = None

    existing = db_get_user(user.id)

    if not existing:
        if ref_id and ref_id != user.id and db_get_user(ref_id):
            db_create_user(user.id, user.username, user.first_name, referred_by=ref_id)
            referrer = db_get_user(ref_id)
            safe_send(
                ref_id,
                f"🎉 <b>New Referral!</b>\n\n"
                f"<b>{user.first_name}</b> joined using your link.\n"
                f"You earned <b>+{REFERRAL_REWARD} TK</b>!\n\n"
                f"💰 New balance: <b>{referrer['balance'] + REFERRAL_REWARD} TK</b>",
            )
            logger.info("Referral: %s → %s", ref_id, user.id)
        else:
            db_create_user(user.id, user.username, user.first_name)
            if ref_id == user.id:
                bot.send_message(msg.chat.id, "⚠️ You cannot refer yourself.", reply_markup=kb_main())

        username_display = f"@{user.username}" if user.username else "N/A"
        safe_send(
            OWNER_ID,
            f"👤 <b>New User Started Bot!</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"• <b>Name:</b>     {user.first_name}\n"
            f"• <b>Username:</b> {username_display}\n"
            f"• <b>User ID:</b>  <code>{user.id}</code>",
        )
    else:
        db_update_user(user.id, username=user.username or "", first_name=user.first_name or "")

    db_user = db_get_user(user.id)
    if db_user and db_user["is_banned"]:
        bot.send_message(msg.chat.id, "⛔ You have been banned by admin.")
        return

    clear_state(user.id)

    welcome = (
        f"👋 <b>Welcome, {user.first_name}!</b>\n\n"
        "🤖 <b>Refer To Earn Bot</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "📌 <b>How it works:</b>\n"
        f"  • Share your unique referral link\n"
        f"  • Earn <b>{REFERRAL_REWARD} TK</b> for every friend who joins\n"
        f"  • Withdraw when you reach <b>{MIN_WITHDRAW} TK</b>\n\n"
        "👇 <b>Choose an option below to get started.</b>"
    )
    bot.send_message(msg.chat.id, welcome, reply_markup=kb_main())


# ─────────────────────────────────────────────
#  MAIN MENU
# ─────────────────────────────────────────────

@bot.message_handler(func=lambda m: m.text == "💰 Refer To Earn")
@ensure_registered
@check_banned
def menu_refer(msg: types.Message):
    clear_state(msg.from_user.id)
    user = db_get_user(msg.from_user.id)
    link = referral_link(msg.from_user.id)

    text = (
        "💰 <b>Refer To Earn</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"🔗 <b>Your Referral Link:</b>\n"
        f"<code>{link}</code>\n\n"
        f"👥 <b>Total Referrals:</b>  {user['referrals']}\n"
        f"💵 <b>Current Balance:</b>  {user['balance']} TK\n\n"
        f"📣 Share your link and earn <b>{REFERRAL_REWARD} TK</b> per referral!"
    )
    bot.send_message(msg.chat.id, text, reply_markup=kb_back())


@bot.message_handler(func=lambda m: m.text == "👤 Admin")
@ensure_registered
@check_banned
def menu_admin(msg: types.Message):
    clear_state(msg.from_user.id)
    text = (
        "👤 <b>Contact Admin</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"For support or any issues, reach out to our admin:\n\n"
        f"💬 <b>Admin:</b> {ADMIN_USERNAME}\n\n"
        "⏱ Response time: usually within 24 hours."
    )
    bot.send_message(msg.chat.id, text, reply_markup=kb_back())


@bot.message_handler(func=lambda m: m.text == "📊 Profile")
@ensure_registered
@check_banned
def menu_profile(msg: types.Message):
    clear_state(msg.from_user.id)
    user   = db_get_user(msg.from_user.id)
    status = "✅ Activated" if user["is_activated"] else "❌ Not Activated"
    joined = str(user["joined_at"])[:10] if user["joined_at"] else "—"

    text = (
        "📊 <b>Your Profile</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"🆔 <b>User ID:</b>       <code>{user['user_id']}</code>\n"
        f"👤 <b>Name:</b>          {user['first_name']}\n"
        f"💵 <b>Balance:</b>       {user['balance']} TK\n"
        f"👥 <b>Referrals:</b>     {user['referrals']}\n"
        f"🔐 <b>Account:</b>       {status}\n"
        f"📅 <b>Joined:</b>        {joined}\n"
    )
    bot.send_message(msg.chat.id, text, reply_markup=kb_back())


# ─────────────────────────────────────────────
#  WITHDRAW FLOW
# ─────────────────────────────────────────────

@bot.message_handler(func=lambda m: m.text == "💳 Withdraw")
@ensure_registered
@check_banned
def menu_withdraw(msg: types.Message):
    clear_state(msg.from_user.id)
    user = db_get_user(msg.from_user.id)

    if user["balance"] < MIN_WITHDRAW:
        bot.send_message(
            msg.chat.id,
            f"⚠️ <b>Insufficient Balance</b>\n\n"
            f"Minimum withdrawal is <b>{MIN_WITHDRAW} TK</b>.\n"
            f"Your balance: <b>{user['balance']} TK</b>\n\n"
            f"Keep referring to earn more! 💪",
            reply_markup=kb_back(),
        )
        return

    if not user["is_activated"]:
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        kb.add(
            types.KeyboardButton("🔓 Activate Your Account"),
            types.KeyboardButton("🔙 Back"),
        )
        bot.send_message(
            msg.chat.id,
            "🔐 <b>Account Not Activated</b>\n\n"
            "You need to activate your account before withdrawing.\n\n"
            f"📌 Activation cost: <b>{ACTIVATION_COST} TK</b> (one-time manual payment)",
            reply_markup=kb,
        )
        return

    if db_pending_withdrawal_exists(msg.from_user.id):
        bot.send_message(
            msg.chat.id,
            "⏳ You already have a <b>pending withdrawal request</b>.\n"
            "Please wait for admin to process it.",
            reply_markup=kb_back(),
        )
        return

    bot.send_message(
        msg.chat.id,
        "💳 <b>Select Payment Method</b>\n\nChoose how you'd like to receive your funds:",
        reply_markup=kb_withdraw_method(),
    )
    set_state(msg.from_user.id, "awaiting_method")


@bot.message_handler(func=lambda m: m.text in ("📱 Bkash", "📲 Nagad"))
@ensure_registered
@check_banned
def handle_method_select(msg: types.Message):
    state, _ = get_state(msg.from_user.id)
    if state != "awaiting_method":
        return

    method = "Bkash" if "Bkash" in msg.text else "Nagad"
    set_state(msg.from_user.id, "awaiting_number", data=method)

    bot.send_message(
        msg.chat.id,
        f"📲 <b>{method} Withdrawal</b>\n\nPlease enter your <b>{method}</b> number:",
        reply_markup=kb_back(),
    )


# ─────────────────────────────────────────────
#  ACTIVATION FLOW
# ─────────────────────────────────────────────

@bot.message_handler(func=lambda m: m.text == "🔓 Activate Your Account")
@ensure_registered
@check_banned
def handle_activate(msg: types.Message):
    user = db_get_user(msg.from_user.id)

    if user["is_activated"]:
        bot.send_message(msg.chat.id, "✅ Your account is already activated!", reply_markup=kb_main())
        return

    if user["balance"] < ACTIVATION_COST:
        bot.send_message(
            msg.chat.id,
            f"⚠️ <b>Insufficient Balance</b>\n\n"
            f"You need at least <b>{ACTIVATION_COST} TK</b> to activate.\n"
            f"Your balance: <b>{user['balance']} TK</b>",
            reply_markup=kb_back(),
        )
        return

    if db_pending_activation_exists(msg.from_user.id):
        bot.send_message(
            msg.chat.id,
            "⏳ You already have a <b>pending activation request</b>.\nPlease wait for admin approval.",
            reply_markup=kb_back(),
        )
        return

    text = (
        "🔓 <b>Account Activation</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"Send <b>{ACTIVATION_COST} TK</b> to:\n\n"
        f"📱 <b>Bkash / Nagad:</b> <code>{ACTIVATION_NUMBER}</code>\n\n"
        "After sending, tap <b>✅ Done</b> and your request will be reviewed.\n\n"
        f"❓ Problems? Contact admin: {ADMIN_USERNAME}"
    )
    bot.send_message(msg.chat.id, text, reply_markup=kb_activation())
    set_state(msg.from_user.id, "awaiting_activation_confirm")


@bot.message_handler(func=lambda m: m.text == "✅ Done")
@ensure_registered
@check_banned
def handle_activation_done(msg: types.Message):
    state, _ = get_state(msg.from_user.id)
    if state != "awaiting_activation_confirm":
        return

    user = db_get_user(msg.from_user.id)
    a_id = db_create_activation(msg.from_user.id)
    clear_state(msg.from_user.id)

    bot.send_message(
        msg.chat.id,
        "✅ <b>Activation Request Submitted!</b>\n\n"
        "Your request has been sent to the admin for review.\n"
        "You'll be notified once it's processed. ⏳",
        reply_markup=kb_main(),
    )

    safe_send(
        OWNER_ID,
        f"🔓 <b>Activation Request #{a_id}</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"👤 <b>User:</b>     {fmt_name(user)}\n"
        f"🆔 <b>User ID:</b>  <code>{user['user_id']}</code>\n"
        f"💵 <b>Balance:</b>  {user['balance']} TK\n"
        f"📅 <b>Time:</b>     {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
        f"Activation payment: <b>{ACTIVATION_COST} TK</b> to {ACTIVATION_NUMBER}",
        reply_markup=kb_inline_approve_reject("activation", a_id),
    )
    logger.info("Activation request #%s from user %s", a_id, msg.from_user.id)


@bot.message_handler(func=lambda m: m.text == "❌ Cancel")
@ensure_registered
@check_banned
def handle_activation_cancel(msg: types.Message):
    state, _ = get_state(msg.from_user.id)
    if state != "awaiting_activation_confirm":
        return
    clear_state(msg.from_user.id)
    bot.send_message(msg.chat.id, "❌ Activation cancelled.", reply_markup=kb_main())


# ─────────────────────────────────────────────
#  BACK BUTTON
# ─────────────────────────────────────────────

@bot.message_handler(func=lambda m: m.text == "🔙 Back")
@ensure_registered
@check_banned
def handle_back(msg: types.Message):
    clear_state(msg.from_user.id)
    bot.send_message(msg.chat.id, "🏠 <b>Main Menu</b>", reply_markup=kb_main())


# ─────────────────────────────────────────────
#  GENERIC TEXT — withdrawal number input
# ─────────────────────────────────────────────

@bot.message_handler(func=lambda m: m.content_type == "text" and m.text and not m.text.startswith("/"))
@ensure_registered
@check_banned
def handle_text(msg: types.Message):
    user_id     = msg.from_user.id
    state, data = get_state(user_id)

    if state == "awaiting_number":
        number = msg.text.strip()

        if not (number.isdigit() and len(number) in (11, 13)):
            bot.send_message(
                msg.chat.id,
                "⚠️ Invalid number. Please enter a valid mobile number (e.g. 01XXXXXXXXX).",
                reply_markup=kb_back(),
            )
            return

        user   = db_get_user(user_id)
        amount = user["balance"]
        method = data or "Unknown"
        w_id   = db_create_withdrawal(user_id, amount, method, number)
        clear_state(user_id)

        bot.send_message(
            msg.chat.id,
            f"✅ <b>Withdrawal Request Submitted!</b>\n\n"
            f"💳 <b>Method:</b>   {method}\n"
            f"📲 <b>Number:</b>   <code>{number}</code>\n"
            f"💵 <b>Amount:</b>   {amount} TK\n\n"
            "⏳ Your request is pending admin approval.",
            reply_markup=kb_main(),
        )

        safe_send(
            OWNER_ID,
            f"💳 <b>Withdrawal Request #{w_id}</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"👤 <b>User:</b>     {fmt_name(user)}\n"
            f"🆔 <b>User ID:</b>  <code>{user['user_id']}</code>\n"
            f"💵 <b>Amount:</b>   {amount} TK\n"
            f"📱 <b>Method:</b>   {method}\n"
            f"📲 <b>Number:</b>   <code>{number}</code>\n"
            f"📅 <b>Time:</b>     {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            reply_markup=kb_inline_approve_reject("withdrawal", w_id),
        )
        logger.info("Withdrawal #%s: user %s, %s TK via %s", w_id, user_id, amount, method)

    else:
        bot.send_message(msg.chat.id, "❓ Use the menu buttons below.", reply_markup=kb_main())


# ─────────────────────────────────────────────
#  CALLBACK QUERIES
# ─────────────────────────────────────────────

@bot.callback_query_handler(func=lambda c: c.data.startswith("withdrawal_"))
@owner_only
def cb_withdrawal(call: types.CallbackQuery):
    parts  = call.data.split("_", 2)
    action = parts[1]
    w_id   = int(parts[2])
    record = db_get_withdrawal(w_id)

    if not record:
        bot.answer_callback_query(call.id, "⚠️ Record not found.")
        return
    if record["status"] != "pending":
        bot.answer_callback_query(call.id, f"Already {record['status']}.")
        return

    if action == "approve":
        db_update_withdrawal(w_id, "approved")
        db_update_user(record["user_id"], balance=0)
        safe_send(
            record["user_id"],
            f"✅ <b>Withdrawal Approved!</b>\n\n"
            f"💵 <b>Amount:</b>   {record['amount']} TK\n"
            f"📱 <b>Method:</b>   {record['method']}\n"
            f"📲 <b>Number:</b>   <code>{record['number']}</code>\n\n"
            "Your payment is on the way. 🎉",
        )
        bot.answer_callback_query(call.id, "✅ Withdrawal approved.")
        bot.edit_message_text(
            f"✅ Withdrawal #{w_id} <b>APPROVED</b>",
            call.message.chat.id, call.message.message_id, parse_mode="HTML",
        )

    elif action == "reject":
        db_update_withdrawal(w_id, "rejected")
        safe_send(
            record["user_id"],
            f"❌ <b>Withdrawal Rejected</b>\n\n"
            f"Your withdrawal of <b>{record['amount']} TK</b> was rejected.\n"
            f"Contact {ADMIN_USERNAME} for details.",
        )
        bot.answer_callback_query(call.id, "❌ Withdrawal rejected.")
        bot.edit_message_text(
            f"❌ Withdrawal #{w_id} <b>REJECTED</b>",
            call.message.chat.id, call.message.message_id, parse_mode="HTML",
        )


@bot.callback_query_handler(func=lambda c: c.data.startswith("activation_"))
@owner_only
def cb_activation(call: types.CallbackQuery):
    parts  = call.data.split("_", 2)
    action = parts[1]
    a_id   = int(parts[2])
    record = db_get_activation(a_id)

    if not record:
        bot.answer_callback_query(call.id, "⚠️ Record not found.")
        return
    if record["status"] != "pending":
        bot.answer_callback_query(call.id, f"Already {record['status']}.")
        return

    if action == "approve":
        db_update_activation(a_id, "approved")
        db_update_user(record["user_id"], is_activated=1)
        safe_send(
            record["user_id"],
            "🎉 <b>Account Activated!</b>\n\n"
            "Your account has been successfully activated. ✅\n\n"
            "You can now make withdrawals! 💰",
        )
        bot.answer_callback_query(call.id, "✅ Activation approved.")
        bot.edit_message_text(
            f"✅ Activation #{a_id} <b>APPROVED</b>",
            call.message.chat.id, call.message.message_id, parse_mode="HTML",
        )

    elif action == "reject":
        db_update_activation(a_id, "rejected")
        safe_send(
            record["user_id"],
            f"❌ <b>Activation Rejected</b>\n\n"
            "Your activation request was rejected.\n"
            f"Contact {ADMIN_USERNAME} for details.",
        )
        bot.answer_callback_query(call.id, "❌ Activation rejected.")
        bot.edit_message_text(
            f"❌ Activation #{a_id} <b>REJECTED</b>",
            call.message.chat.id, call.message.message_id, parse_mode="HTML",
        )


# ─────────────────────────────────────────────
#  ADMIN COMMANDS
# ─────────────────────────────────────────────

@bot.message_handler(commands=["stats"])
def cmd_stats(msg: types.Message):
    if msg.from_user.id != OWNER_ID:
        return
    with _DBConn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM users")
            total_users = cur.fetchone()["cnt"]
            cur.execute("SELECT COUNT(*) AS cnt FROM users WHERE is_activated = 1")
            activated = cur.fetchone()["cnt"]
            cur.execute("SELECT COUNT(*) AS cnt FROM withdrawals WHERE status = 'pending'")
            pending_w = cur.fetchone()["cnt"]
            cur.execute("SELECT COALESCE(SUM(amount), 0) AS total FROM withdrawals WHERE status = 'approved'")
            total_withdrawn = cur.fetchone()["total"]
            cur.execute("SELECT COUNT(*) AS cnt FROM activations WHERE status = 'pending'")
            pending_a = cur.fetchone()["cnt"]

    bot.send_message(
        msg.chat.id,
        "📊 <b>Bot Statistics</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"👥 <b>Total Users:</b>         {total_users}\n"
        f"✅ <b>Activated:</b>           {activated}\n"
        f"💳 <b>Pending Withdrawals:</b> {pending_w}\n"
        f"🏦 <b>Total Paid Out:</b>      {total_withdrawn} TK\n"
        f"🔓 <b>Pending Activations:</b> {pending_a}\n",
    )


@bot.message_handler(commands=["leaderboard"])
def cmd_leaderboard(msg: types.Message):
    if msg.from_user.id != OWNER_ID:
        return
    top = db_get_top_referrers(10)
    if not top:
        bot.send_message(msg.chat.id, "No data yet.")
        return
    lines  = ["🏆 <b>Top Referrers</b>\n━━━━━━━━━━━━━━━━"]
    medals = ["🥇", "🥈", "🥉"] + ["🔹"] * 7
    for i, row in enumerate(top):
        name = f"@{row['username']}" if row["username"] else row["first_name"]
        lines.append(f"{medals[i]} {name}  —  <b>{row['referrals']} referrals</b>")
    bot.send_message(msg.chat.id, "\n".join(lines))


@bot.message_handler(commands=["addbalance"])
def cmd_add_balance(msg: types.Message):
    if msg.from_user.id != OWNER_ID:
        return
    try:
        _, uid_str, amount_str = msg.text.split()
        uid    = int(uid_str)
        amount = int(amount_str)
        user   = db_get_user(uid)
        if not user:
            bot.send_message(msg.chat.id, "❌ User not found.")
            return
        new_bal = user["balance"] + amount
        db_update_user(uid, balance=new_bal)
        bot.send_message(msg.chat.id, f"✅ Added {amount} TK to user {uid}. New balance: {new_bal} TK")
        safe_send(uid, f"💰 Admin added <b>{amount} TK</b> to your account. Balance: <b>{new_bal} TK</b>")
    except (ValueError, IndexError):
        bot.send_message(msg.chat.id, "Usage: /addbalance <user_id> <amount>")


@bot.message_handler(commands=["activate"])
def cmd_force_activate(msg: types.Message):
    if msg.from_user.id != OWNER_ID:
        return
    try:
        uid = int(msg.text.split()[1])
        db_update_user(uid, is_activated=1)
        bot.send_message(msg.chat.id, f"✅ User {uid} activated.")
        safe_send(uid, "🎉 Your account has been <b>activated</b> by admin!")
    except (ValueError, IndexError):
        bot.send_message(msg.chat.id, "Usage: /activate <user_id>")


@bot.message_handler(commands=["ban"])
def cmd_ban(msg: types.Message):
    if msg.from_user.id != OWNER_ID:
        return
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.send_message(msg.chat.id, "Usage: /ban @username")
        return
    user = db_get_user_by_username(parts[1].strip())
    if not user:
        bot.send_message(msg.chat.id, f"❌ User <b>{parts[1]}</b> not found.", parse_mode="HTML")
        return
    if user["is_banned"]:
        bot.send_message(msg.chat.id, f"⚠️ Already banned.", parse_mode="HTML")
        return
    db_update_user(user["user_id"], is_banned=1)
    bot.send_message(msg.chat.id, f"✅ <b>{fmt_name(user)}</b> banned.", parse_mode="HTML")
    safe_send(user["user_id"], "⛔ You have been banned by admin.")


@bot.message_handler(commands=["unban"])
def cmd_unban(msg: types.Message):
    if msg.from_user.id != OWNER_ID:
        return
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.send_message(msg.chat.id, "Usage: /unban @username")
        return
    user = db_get_user_by_username(parts[1].strip())
    if not user:
        bot.send_message(msg.chat.id, f"❌ User not found.", parse_mode="HTML")
        return
    if not user["is_banned"]:
        bot.send_message(msg.chat.id, f"⚠️ User is not banned.")
        return
    db_update_user(user["user_id"], is_banned=0)
    bot.send_message(msg.chat.id, f"✅ <b>{fmt_name(user)}</b> unbanned.", parse_mode="HTML")
    safe_send(user["user_id"], "✅ You have been unbanned by admin.")


@bot.message_handler(commands=["add"])
def cmd_add(msg: types.Message):
    if msg.from_user.id != OWNER_ID:
        return
    parts = msg.text.split(maxsplit=2)
    if len(parts) < 3:
        bot.send_message(msg.chat.id, "Usage: /add @username &lt;amount&gt;", parse_mode="HTML")
        return
    try:
        amount = int(parts[2].strip())
    except ValueError:
        bot.send_message(msg.chat.id, "⚠️ Amount must be a number.")
        return
    if amount <= 0:
        bot.send_message(msg.chat.id, "⚠️ Amount must be greater than 0.")
        return
    user = db_get_user_by_username(parts[1].strip())
    if not user:
        bot.send_message(msg.chat.id, "❌ User not found.", parse_mode="HTML")
        return
    new_bal = user["balance"] + amount
    db_update_user(user["user_id"], balance=new_bal)
    bot.send_message(msg.chat.id, f"✅ Added <b>{amount} TK</b> to <b>{fmt_name(user)}</b>. New balance: <b>{new_bal} TK</b>", parse_mode="HTML")
    safe_send(user["user_id"], f"💰 Admin added <b>{amount} TK</b> to your balance.\nNew balance: <b>{new_bal} TK</b>")


@bot.message_handler(commands=["remove"])
def cmd_remove(msg: types.Message):
    if msg.from_user.id != OWNER_ID:
        return
    parts = msg.text.split(maxsplit=2)
    if len(parts) < 3:
        bot.send_message(msg.chat.id, "Usage: /remove @username &lt;amount&gt;", parse_mode="HTML")
        return
    try:
        amount = int(parts[2].strip())
    except ValueError:
        bot.send_message(msg.chat.id, "⚠️ Amount must be a number.")
        return
    if amount <= 0:
        bot.send_message(msg.chat.id, "⚠️ Amount must be greater than 0.")
        return
    user = db_get_user_by_username(parts[1].strip())
    if not user:
        bot.send_message(msg.chat.id, "❌ User not found.", parse_mode="HTML")
        return
    new_bal = max(0, user["balance"] - amount)
    db_update_user(user["user_id"], balance=new_bal)
    bot.send_message(msg.chat.id, f"✅ Removed <b>{amount} TK</b> from <b>{fmt_name(user)}</b>. New balance: <b>{new_bal} TK</b>", parse_mode="HTML")
    safe_send(user["user_id"], f"💸 Admin removed <b>{amount} TK</b> from your balance.\nNew balance: <b>{new_bal} TK</b>")


@bot.message_handler(commands=["check"])
def cmd_check(msg: types.Message):
    if msg.from_user.id != OWNER_ID:
        return
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.send_message(msg.chat.id, "Usage: /check @username")
        return
    user = db_get_user_by_username(parts[1].strip())
    if not user:
        bot.send_message(msg.chat.id, "❌ User not found.", parse_mode="HTML")
        return
    withdrawals_count = db_count_withdrawals(user["user_id"])
    pending_label     = "Yes" if db_count_pending_activations(user["user_id"]) > 0 else "No"
    banned_status     = "Yes" if user["is_banned"] else "No"

    bot.send_message(
        msg.chat.id,
        "👤 <b>User Info</b>\n"
        "━━━━━━━━━━━━━━━\n"
        f"🆔 <b>User ID:</b>       <code>{user['user_id']}</code>\n"
        f"📛 <b>Name:</b>          {user['first_name']}\n"
        f"💰 <b>Balance:</b>       {user['balance']} TK\n"
        f"💸 <b>Withdrawals:</b>   {withdrawals_count}\n"
        f"👥 <b>Referrals:</b>     {user['referrals']}\n"
        f"⏳ <b>Pending Task:</b>  {pending_label}\n"
        f"⛔ <b>Banned:</b>        {banned_status}\n",
    )


@bot.message_handler(commands=["broadcast"])
def cmd_broadcast(msg: types.Message):
    if msg.from_user.id != OWNER_ID:
        return
    if len(msg.text.split()) < 2:
        bot.send_message(msg.chat.id, "❌ Usage: <code>/broadcast your message here</code>", parse_mode="HTML")
        return
    broadcast_text = msg.text.split(maxsplit=1)[1].strip()
    if not broadcast_text:
        bot.send_message(msg.chat.id, "❌ Message cannot be empty.")
        return

    with _DBConn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id FROM users")
            users = cur.fetchall()

    total = len(users)
    sent = failed = 0
    for row in users:
        try:
            bot.send_message(row["user_id"], broadcast_text, parse_mode="HTML")
            sent += 1
        except Exception as e:
            failed += 1
            logger.warning("Broadcast failed for %s: %s", row["user_id"], e)

    bot.send_message(
        msg.chat.id,
        f"✅ <b>Broadcast Done!</b>\n"
        f"📨 Sent: {sent}/{total}\n"
        f"❌ Failed: {failed}",
        parse_mode="HTML",
    )


@bot.message_handler(commands=["msg"])
def cmd_msg(msg: types.Message):
    if msg.from_user.id != OWNER_ID:
        return
    parts = msg.text.split(maxsplit=2)
    if len(parts) < 3:
        bot.send_message(msg.chat.id, "Usage: /msg @username &lt;message&gt;", parse_mode="HTML")
        return
    user = db_get_user_by_username(parts[1].strip())
    if not user:
        bot.send_message(msg.chat.id, "❌ User not found.", parse_mode="HTML")
        return
    try:
        bot.send_message(user["user_id"], parts[2].strip(), parse_mode="HTML")
        bot.send_message(msg.chat.id, f"✅ Message sent to <b>{fmt_name(user)}</b>.", parse_mode="HTML")
    except Exception as exc:
        bot.send_message(msg.chat.id, f"❌ Failed: {exc}", parse_mode="HTML")


# ─────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    init_db(retries=10, delay=5)
    logger.info("Bot starting...")
    bot.infinity_polling(timeout=30, long_polling_timeout=20)
