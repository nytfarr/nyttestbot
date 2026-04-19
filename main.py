"""
Telegram Refer-to-Earn Bot
Author: Production-Ready Single File
Library: pyTelegramBotAPI
Database: SQLite
"""

import os
import logging
import sqlite3
import threading
from datetime import datetime
from functools import wraps

import telebot
from telebot import types

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────

BOT_TOKEN   = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
OWNER_ID    = int(os.getenv("OWNER_ID", "8499435987"))
ADMIN_USERNAME = "@SoulFoex"
BOT_USERNAME   = os.getenv("BOT_USERNAME", "YourBotUsername")  # without @

REFERRAL_REWARD   = 20    # TK per referral
MIN_WITHDRAW      = 100   # TK
ACTIVATION_COST   = 50    # TK
ACTIVATION_NUMBER = "01705930972"

DB_PATH = "bot.db"

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

_db_lock = threading.Lock()


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _db_lock, get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                user_id       INTEGER PRIMARY KEY,
                username      TEXT,
                first_name    TEXT,
                balance       INTEGER DEFAULT 0,
                referrals     INTEGER DEFAULT 0,
                referred_by   INTEGER DEFAULT NULL,
                is_activated  INTEGER DEFAULT 0,
                is_banned     INTEGER DEFAULT 0,
                joined_at     TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS withdrawals (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id       INTEGER NOT NULL,
                amount        INTEGER NOT NULL,
                method        TEXT NOT NULL,
                number        TEXT NOT NULL,
                status        TEXT DEFAULT 'pending',
                created_at    TEXT DEFAULT (datetime('now')),
                resolved_at   TEXT DEFAULT NULL
            );

            CREATE TABLE IF NOT EXISTS activations (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id       INTEGER NOT NULL,
                status        TEXT DEFAULT 'pending',
                created_at    TEXT DEFAULT (datetime('now')),
                resolved_at   TEXT DEFAULT NULL
            );

            CREATE TABLE IF NOT EXISTS user_states (
                user_id       INTEGER PRIMARY KEY,
                state         TEXT DEFAULT NULL,
                data          TEXT DEFAULT NULL
            );
        """)
        # Migrate existing databases that may not have is_banned column
        try:
            conn.execute("ALTER TABLE users ADD COLUMN is_banned INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass  # Column already exists
    logger.info("Database initialised.")


# ── User helpers ──────────────────────────────

def db_get_user(user_id: int) -> sqlite3.Row | None:
    with _db_lock, get_conn() as conn:
        return conn.execute(
            "SELECT * FROM users WHERE user_id = ?", (user_id,)
        ).fetchone()


def db_get_user_by_username(username: str) -> sqlite3.Row | None:
    username = username.lstrip("@")
    with _db_lock, get_conn() as conn:
        return conn.execute(
            "SELECT * FROM users WHERE LOWER(username) = LOWER(?)", (username,)
        ).fetchone()


def db_create_user(user_id: int, username: str, first_name: str, referred_by: int | None = None):
    with _db_lock, get_conn() as conn:
        conn.execute(
            """INSERT OR IGNORE INTO users (user_id, username, first_name, referred_by)
               VALUES (?, ?, ?, ?)""",
            (user_id, username or "", first_name or "", referred_by),
        )
        if referred_by:
            conn.execute(
                "UPDATE users SET balance = balance + ?, referrals = referrals + 1 WHERE user_id = ?",
                (REFERRAL_REWARD, referred_by),
            )


def db_update_user(user_id: int, **kwargs):
    fields = ", ".join(f"{k} = ?" for k in kwargs)
    values = list(kwargs.values()) + [user_id]
    with _db_lock, get_conn() as conn:
        conn.execute(f"UPDATE users SET {fields} WHERE user_id = ?", values)


def db_get_top_referrers(limit: int = 10):
    with _db_lock, get_conn() as conn:
        return conn.execute(
            "SELECT user_id, first_name, username, referrals FROM users ORDER BY referrals DESC LIMIT ?",
            (limit,),
        ).fetchall()


# ── Withdrawal helpers ────────────────────────

def db_create_withdrawal(user_id: int, amount: int, method: str, number: str) -> int:
    with _db_lock, get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO withdrawals (user_id, amount, method, number) VALUES (?, ?, ?, ?)",
            (user_id, amount, method, number),
        )
        return cur.lastrowid


def db_get_withdrawal(w_id: int) -> sqlite3.Row | None:
    with _db_lock, get_conn() as conn:
        return conn.execute(
            "SELECT * FROM withdrawals WHERE id = ?", (w_id,)
        ).fetchone()


def db_update_withdrawal(w_id: int, status: str):
    with _db_lock, get_conn() as conn:
        conn.execute(
            "UPDATE withdrawals SET status = ?, resolved_at = datetime('now') WHERE id = ?",
            (status, w_id),
        )


def db_pending_withdrawal_exists(user_id: int) -> bool:
    with _db_lock, get_conn() as conn:
        row = conn.execute(
            "SELECT id FROM withdrawals WHERE user_id = ? AND status = 'pending' LIMIT 1",
            (user_id,),
        ).fetchone()
        return row is not None


# ── Activation helpers ────────────────────────

def db_create_activation(user_id: int) -> int:
    with _db_lock, get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO activations (user_id) VALUES (?)", (user_id,)
        )
        return cur.lastrowid


def db_get_activation(a_id: int) -> sqlite3.Row | None:
    with _db_lock, get_conn() as conn:
        return conn.execute(
            "SELECT * FROM activations WHERE id = ?", (a_id,)
        ).fetchone()


def db_update_activation(a_id: int, status: str):
    with _db_lock, get_conn() as conn:
        conn.execute(
            "UPDATE activations SET status = ?, resolved_at = datetime('now') WHERE id = ?",
            (status, a_id),
        )


def db_pending_activation_exists(user_id: int) -> bool:
    with _db_lock, get_conn() as conn:
        row = conn.execute(
            "SELECT id FROM activations WHERE user_id = ? AND status = 'pending' LIMIT 1",
            (user_id,),
        ).fetchone()
        return row is not None


# ── State helpers ─────────────────────────────

def set_state(user_id: int, state: str, data: str = ""):
    with _db_lock, get_conn() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO user_states (user_id, state, data) VALUES (?, ?, ?)",
            (user_id, state, data),
        )


def get_state(user_id: int) -> tuple[str | None, str | None]:
    with _db_lock, get_conn() as conn:
        row = conn.execute(
            "SELECT state, data FROM user_states WHERE user_id = ?", (user_id,)
        ).fetchone()
        return (row["state"], row["data"]) if row else (None, None)


def clear_state(user_id: int):
    with _db_lock, get_conn() as conn:
        conn.execute("DELETE FROM user_states WHERE user_id = ?", (user_id,))


# ── Withdrawal/activation count helpers (for /check) ─────────────────────────

def db_count_withdrawals(user_id: int) -> int:
    with _db_lock, get_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM withdrawals WHERE user_id = ? AND status = 'approved'",
            (user_id,),
        ).fetchone()
        return row[0] if row else 0


def db_count_pending_activations(user_id: int) -> int:
    with _db_lock, get_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM activations WHERE user_id = ? AND status = 'pending'",
            (user_id,),
        ).fetchone()
        return row[0] if row else 0


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
        types.InlineKeyboardButton("❌ Reject",  callback_data=f"{action}_reject_{record_id}"),
    )
    return kb


# ─────────────────────────────────────────────
#  DECORATORS
# ─────────────────────────────────────────────

def owner_only(func):
    """Restrict handler to OWNER_ID."""
    @wraps(func)
    def wrapper(call_or_msg, *args, **kwargs):
        uid = (
            call_or_msg.from_user.id
            if hasattr(call_or_msg, "from_user")
            else call_or_msg.message.from_user.id
        )
        if uid != OWNER_ID:
            bot.answer_callback_query(call_or_msg.id, "⛔ Unauthorised.")
            return
        return func(call_or_msg, *args, **kwargs)
    return wrapper


def ensure_registered(func):
    """Auto-register user before running handler."""
    @wraps(func)
    def wrapper(msg, *args, **kwargs):
        user = msg.from_user
        if not db_get_user(user.id):
            db_create_user(user.id, user.username, user.first_name)
        return func(msg, *args, **kwargs)
    return wrapper


def check_banned(func):
    """Block banned users from using bot features."""
    @wraps(func)
    def wrapper(msg, *args, **kwargs):
        user = db_get_user(msg.from_user.id)
        if user and user["is_banned"]:
            bot.send_message(
                msg.chat.id,
                "⛔ You have been banned by admin.",
            )
            return
        return func(msg, *args, **kwargs)
    return wrapper


# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────

def referral_link(user_id: int) -> str:
    return f"[t.me](https://t.me/{BOT_USERNAME}?start=ref_{user_id})"


def fmt_name(row: sqlite3.Row) -> str:
    name = row["first_name"] or "User"
    return f"@{row['username']}" if row["username"] else name


def safe_send(user_id: int, text: str, **kwargs):
    """Send a message, silently swallow blocked/deactivated user errors."""
    try:
        bot.send_message(user_id, text, **kwargs)
    except Exception as exc:
        logger.warning("Could not send to %s: %s", user_id, exc)


# ─────────────────────────────────────────────
#  /start  ─  entry point
# ─────────────────────────────────────────────

@bot.message_handler(commands=["start"])
def cmd_start(msg: types.Message):
    user   = msg.from_user
    args   = msg.text.split(maxsplit=1)
    param  = args[1] if len(args) > 1 else ""
    ref_id = None

    # Parse referral parameter
    if param.startswith("ref_"):
        try:
            ref_id = int(param[4:])
        except ValueError:
            ref_id = None

    existing = db_get_user(user.id)

    if not existing:
        # Validate referral
        if ref_id and ref_id != user.id and db_get_user(ref_id):
            db_create_user(user.id, user.username, user.first_name, referred_by=ref_id)
            # Notify referrer
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
                bot.send_message(
                    msg.chat.id,
                    "⚠️ You cannot refer yourself.",
                    reply_markup=kb_main(),
                )

        # Notify admin of new user
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
        # Already registered — refresh name silently
        db_update_user(user.id, username=user.username or "", first_name=user.first_name or "")

    # Check if banned before showing menu
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
#  MAIN MENU TEXT HANDLERS
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
    user = db_get_user(msg.from_user.id)
    status = "✅ Activated" if user["is_activated"] else "❌ Not Activated"
    joined = user["joined_at"][:10] if user["joined_at"] else "—"

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

    # CASE 1 — insufficient balance
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

    # CASE 2 — not activated
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

    # CASE 3 — eligible
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
        "💳 <b>Select Payment Method</b>\n\n"
        "Choose how you'd like to receive your funds:",
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
        f"📲 <b>{method} Withdrawal</b>\n\n"
        f"Please enter your <b>{method}</b> number:",
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
            "⏳ You already have a <b>pending activation request</b>.\n"
            "Please wait for admin approval.",
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

    user  = db_get_user(msg.from_user.id)
    a_id  = db_create_activation(msg.from_user.id)
    clear_state(msg.from_user.id)

    bot.send_message(
        msg.chat.id,
        "✅ <b>Activation Request Submitted!</b>\n\n"
        "Your request has been sent to the admin for review.\n"
        "You'll be notified once it's processed. ⏳",
        reply_markup=kb_main(),
    )

    # Notify admin
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
    bot.send_message(
        msg.chat.id,
        "🏠 <b>Main Menu</b>",
        reply_markup=kb_main(),
    )


# ─────────────────────────────────────────────
#  GENERIC TEXT — catches number input for withdrawal
# ─────────────────────────────────────────────

@bot.message_handler(
    func=lambda m: m.content_type == "text"
                  and m.text
                  and not m.text.startswith("/")
)
@ensure_registered
@check_banned
def handle_text(msg: types.Message):
    user_id      = msg.from_user.id
    state, data  = get_state(user_id)

    if state == "awaiting_number":
        number = msg.text.strip()

        # Basic number validation (Bangladeshi numbers)
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

        # Notify admin
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
        logger.info("Withdrawal #%s: user %s, amount %s TK via %s", w_id, user_id, amount, method)

    else:
        # Unrecognised input → nudge back to menu
        bot.send_message(
            msg.chat.id,
            "❓ Use the menu buttons below.",
            reply_markup=kb_main(),
        )


# ─────────────────────────────────────────────
#  CALLBACK QUERIES — admin approval panel
# ─────────────────────────────────────────────

@bot.callback_query_handler(func=lambda c: c.data.startswith("withdrawal_"))
@owner_only
def cb_withdrawal(call: types.CallbackQuery):
    _, action, w_id_str = call.data.split("_", 2)
    w_id = int(w_id_str)
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
            call.message.chat.id,
            call.message.message_id,
            parse_mode="HTML",
        )
        logger.info("Withdrawal #%s approved by admin.", w_id)

    elif action == "reject":
        db_update_withdrawal(w_id, "rejected")

        safe_send(
            record["user_id"],
            f"❌ <b>Withdrawal Rejected</b>\n\n"
            f"Your withdrawal request of <b>{record['amount']} TK</b> was rejected.\n"
            f"Contact {ADMIN_USERNAME} for details.",
        )
        bot.answer_callback_query(call.id, "❌ Withdrawal rejected.")
        bot.edit_message_text(
            f"❌ Withdrawal #{w_id} <b>REJECTED</b>",
            call.message.chat.id,
            call.message.message_id,
            parse_mode="HTML",
        )
        logger.info("Withdrawal #%s rejected by admin.", w_id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("activation_"))
@owner_only
def cb_activation(call: types.CallbackQuery):
    _, action, a_id_str = call.data.split("_", 2)
    a_id   = int(a_id_str)
    record = db_get_activation(a_id)

    if not record:
        bot.answer_callback_query(call.id, "⚠️ Record not found.")
        return

    if record["status"] != "pending":
        bot.answer_callback_query(call.id, f"Already {record['status']}.")
        return

    if action == "approve":
        db_update_activation(a_id, "approved")
        db_update_user(record["user_id"], is_activated=1)   # ← শুধু এই লাইন

        safe_send(
            record["user_id"],
            f"🎉 <b>Account Activated!</b>\n\n"
            f"Your account has been successfully activated. ✅\n\n"
            "You can now make withdrawals! 💰",
        )
        bot.answer_callback_query(call.id, "✅ Activation approved.")
        bot.edit_message_text(
            f"✅ Activation #{a_id} <b>APPROVED</b>",
            call.message.chat.id,
            call.message.message_id,
            parse_mode="HTML",
        )
        logger.info("Activation #%s approved.", a_id)

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
            call.message.chat.id,
            call.message.message_id,
            parse_mode="HTML",
        )
        logger.info("Activation #%s rejected.", a_id)


# ─────────────────────────────────────────────
#  ADMIN COMMANDS
# ─────────────────────────────────────────────

@bot.message_handler(commands=["stats"])
def cmd_stats(msg: types.Message):
    if msg.from_user.id != OWNER_ID:
        return

    with _db_lock, get_conn() as conn:
        total_users     = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        activated       = conn.execute("SELECT COUNT(*) FROM users WHERE is_activated = 1").fetchone()[0]
        pending_w       = conn.execute("SELECT COUNT(*) FROM withdrawals WHERE status = 'pending'").fetchone()[0]
        total_withdrawn = conn.execute("SELECT COALESCE(SUM(amount),0) FROM withdrawals WHERE status = 'approved'").fetchone()[0]
        pending_a       = conn.execute("SELECT COUNT(*) FROM activations WHERE status = 'pending'").fetchone()[0]

    text = (
        "📊 <b>Bot Statistics</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"👥 <b>Total Users:</b>        {total_users}\n"
        f"✅ <b>Activated:</b>          {activated}\n"
        f"💳 <b>Pending Withdrawals:</b> {pending_w}\n"
        f"🏦 <b>Total Paid Out:</b>     {total_withdrawn} TK\n"
        f"🔓 <b>Pending Activations:</b> {pending_a}\n"
    )
    bot.send_message(msg.chat.id, text)


@bot.message_handler(commands=["leaderboard"])
def cmd_leaderboard(msg: types.Message):
    if msg.from_user.id != OWNER_ID:
        return
    top = db_get_top_referrers(10)
    if not top:
        bot.send_message(msg.chat.id, "No data yet.")
        return

    lines = ["🏆 <b>Top Referrers</b>\n━━━━━━━━━━━━━━━━"]
    medals = ["🥇", "🥈", "🥉"] + ["🔹"] * 7
    for i, row in enumerate(top):
        name = f"@{row['username']}" if row["username"] else row["first_name"]
        lines.append(f"{medals[i]} {name}  —  <b>{row['referrals']} referrals</b>")

    bot.send_message(msg.chat.id, "\n".join(lines), reply_markup=kb_back())


@bot.message_handler(commands=["addbalance"])
def cmd_add_balance(msg: types.Message):
    """Admin: /addbalance <user_id> <amount>"""
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
    """Admin: /activate <user_id>"""
    if msg.from_user.id != OWNER_ID:
        return
    try:
        uid = int(msg.text.split()[1])
        db_update_user(uid, is_activated=1)
        bot.send_message(msg.chat.id, f"✅ User {uid} activated.")
        safe_send(uid, "🎉 Your account has been <b>activated</b> by admin!")
    except (ValueError, IndexError):
        bot.send_message(msg.chat.id, "Usage: /activate <user_id>")


# ─────────────────────────────────────────────
#  NEW ADMIN COMMANDS
# ─────────────────────────────────────────────

@bot.message_handler(commands=["ban"])
def cmd_ban(msg: types.Message):
    """Admin: /ban @username"""
    if msg.from_user.id != OWNER_ID:
        return
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.send_message(msg.chat.id, "Usage: /ban @username")
        return

    target_username = parts[1].strip()
    user = db_get_user_by_username(target_username)

    if not user:
        bot.send_message(msg.chat.id, f"❌ User <b>{target_username}</b> not found.", parse_mode="HTML")
        return

    if user["is_banned"]:
        bot.send_message(msg.chat.id, f"⚠️ User <b>{target_username}</b> is already banned.", parse_mode="HTML")
        return

    db_update_user(user["user_id"], is_banned=1)
    bot.send_message(
        msg.chat.id,
        f"✅ User <b>{fmt_name(user)}</b> (<code>{user['user_id']}</code>) has been banned.",
        parse_mode="HTML",
    )
    safe_send(user["user_id"], "⛔ You have been banned by admin.")
    logger.info("User %s banned by admin.", user["user_id"])


@bot.message_handler(commands=["unban"])
def cmd_unban(msg: types.Message):
    """Admin: /unban @username"""
    if msg.from_user.id != OWNER_ID:
        return
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.send_message(msg.chat.id, "Usage: /unban @username")
        return

    target_username = parts[1].strip()
    user = db_get_user_by_username(target_username)

    if not user:
        bot.send_message(msg.chat.id, f"❌ User <b>{target_username}</b> not found.", parse_mode="HTML")
        return

    if not user["is_banned"]:
        bot.send_message(msg.chat.id, f"⚠️ User <b>{target_username}</b> is not banned.", parse_mode="HTML")
        return

    db_update_user(user["user_id"], is_banned=0)
    bot.send_message(
        msg.chat.id,
        f"✅ User <b>{fmt_name(user)}</b> (<code>{user['user_id']}</code>) has been unbanned.",
        parse_mode="HTML",
    )
    safe_send(user["user_id"], "✅ You have been unbanned by admin.")
    logger.info("User %s unbanned by admin.", user["user_id"])


@bot.message_handler(commands=["add"])
def cmd_add(msg: types.Message):
    """Admin: /add @username <amount>"""
    if msg.from_user.id != OWNER_ID:
        return
    parts = msg.text.split(maxsplit=2)
    if len(parts) < 3:
        bot.send_message(msg.chat.id, "Usage: /add @username &lt;amount&gt;", parse_mode="HTML")
        return

    target_username = parts[1].strip()
    try:
        amount = int(parts[2].strip())
    except ValueError:
        bot.send_message(msg.chat.id, "⚠️ Amount must be a number.")
        return

    if amount <= 0:
        bot.send_message(msg.chat.id, "⚠️ Amount must be greater than 0.")
        return

    user = db_get_user_by_username(target_username)
    if not user:
        bot.send_message(msg.chat.id, f"❌ User <b>{target_username}</b> not found.", parse_mode="HTML")
        return

    new_bal = user["balance"] + amount
    db_update_user(user["user_id"], balance=new_bal)

    bot.send_message(
        msg.chat.id,
        f"✅ Added <b>{amount} TK</b> to <b>{fmt_name(user)}</b>.\n"
        f"New balance: <b>{new_bal} TK</b>",
        parse_mode="HTML",
    )
    safe_send(
        user["user_id"],
        f"💰 Admin added <b>{amount} TK</b> to your balance.\n"
        f"New balance: <b>{new_bal} TK</b>",
    )
    logger.info("Admin added %s TK to user %s. New balance: %s", amount, user["user_id"], new_bal)


@bot.message_handler(commands=["remove"])
def cmd_remove(msg: types.Message):
    """Admin: /remove @username <amount>"""
    if msg.from_user.id != OWNER_ID:
        return
    parts = msg.text.split(maxsplit=2)
    if len(parts) < 3:
        bot.send_message(msg.chat.id, "Usage: /remove @username &lt;amount&gt;", parse_mode="HTML")
        return

    target_username = parts[1].strip()
    try:
        amount = int(parts[2].strip())
    except ValueError:
        bot.send_message(msg.chat.id, "⚠️ Amount must be a number.")
        return

    if amount <= 0:
        bot.send_message(msg.chat.id, "⚠️ Amount must be greater than 0.")
        return

    user = db_get_user_by_username(target_username)
    if not user:
        bot.send_message(msg.chat.id, f"❌ User <b>{target_username}</b> not found.", parse_mode="HTML")
        return

    new_bal = max(0, user["balance"] - amount)
    db_update_user(user["user_id"], balance=new_bal)

    bot.send_message(
        msg.chat.id,
        f"✅ Removed <b>{amount} TK</b> from <b>{fmt_name(user)}</b>.\n"
        f"New balance: <b>{new_bal} TK</b>",
        parse_mode="HTML",
    )
    safe_send(
        user["user_id"],
        f"💸 Admin removed <b>{amount} TK</b> from your balance.\n"
        f"New balance: <b>{new_bal} TK</b>",
    )
    logger.info("Admin removed %s TK from user %s. New balance: %s", amount, user["user_id"], new_bal)


@bot.message_handler(commands=["check"])
def cmd_check(msg: types.Message):
    """Admin: /check @username"""
    if msg.from_user.id != OWNER_ID:
        return
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.send_message(msg.chat.id, "Usage: /check @username")
        return

    target_username = parts[1].strip()
    user = db_get_user_by_username(target_username)

    if not user:
        bot.send_message(msg.chat.id, f"❌ User <b>{target_username}</b> not found.", parse_mode="HTML")
        return

    withdrawals_count = db_count_withdrawals(user["user_id"])
    pending_task      = db_count_pending_activations(user["user_id"]) > 0
    banned_status     = "Yes" if user["is_banned"] else "No"
    pending_label     = "Yes" if pending_task else "No"

    text = (
        "👤 <b>User Info</b>\n"
        "━━━━━━━━━━━━━━━\n"
        f"🆔 <b>User ID:</b>       <code>{user['user_id']}</code>\n"
        f"📛 <b>Name:</b>          {user['first_name']}\n"
        f"💰 <b>Balance:</b>       {user['balance']} TK\n"
        f"✅ <b>Tasks Done:</b>    0\n"
        f"💸 <b>Withdrawals:</b>   {withdrawals_count}\n"
        f"👥 <b>Referrals:</b>     {user['referrals']}\n"
        f"⏳ <b>Pending Task:</b>  {pending_label}\n"
        f"⛔ <b>Banned:</b>        {banned_status}\n"
    )
    bot.send_message(msg.chat.id, text)
    logger.info("Admin checked info for user %s", user["user_id"])


@bot.message_handler(commands=["broadcast"])
def cmd_broadcast(msg: types.Message):
    """Admin: /broadcast <message> — Send message to ALL users"""
    if msg.from_user.id != OWNER_ID:
        return

    # সঠিকভাবে message extract করা (পুরো text নেওয়া)
    if len(msg.text.split()) < 2:
        bot.send_message(
            msg.chat.id,
            "❌ Usage: <code>/broadcast তোমার মেসেজ এখানে লিখো</code>\n\n"
            "উদাহরণ:\n"
            "<code>/broadcast Hello everyone! New update is live 🔥\n"
            "এখন থেকে withdraw minimum 100 TK করা যাবে।</code>",
            parse_mode="HTML"
        )
        return

    # এখানে সঠিক লাইন
    broadcast_text = msg.text.split(maxsplit=1)[1].strip()

    if not broadcast_text:
        bot.send_message(msg.chat.id, "❌ Message খালি দিতে পারবে না।")
        return

    # সব user এর ID নিয়ে আসা
    with _db_lock, get_conn() as conn:
        users = conn.execute("SELECT user_id FROM users").fetchall()

    if not users:
        bot.send_message(msg.chat.id, "❌ No users found in database.")
        return

    total = len(users)
    sent = 0
    failed = 0

    for row in users:
        user_id = row["user_id"]
        try:
            bot.send_message(user_id, broadcast_text, parse_mode="HTML")
            sent += 1
        except Exception as e:
            failed += 1
            logger.warning(f"Broadcast failed for user {user_id}: {e}")
            continue

    # Admin কে report
    bot.send_message(
        msg.chat.id,
        f"✅ <b>Broadcast Successful!</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📨 <b>Sent to:</b> {sent}/{total} users\n"
        f"❌ <b>Failed:</b> {failed} users\n\n"
        f"📝 <b>Message:</b>\n"
        f"{broadcast_text[:400]}{'...' if len(broadcast_text) > 400 else ''}",
        parse_mode="HTML"
    )

    logger.info(f"🔊 Broadcast sent to {sent}/{total} users (failed: {failed})")


@bot.message_handler(commands=["msg"])
def cmd_msg(msg: types.Message):
    """Admin: /msg @username <message>"""
    if msg.from_user.id != OWNER_ID:
        return

    parts = msg.text.split(maxsplit=2)
    if len(parts) < 3:
        bot.send_message(msg.chat.id, "Usage: /msg @username &lt;message&gt;", parse_mode="HTML")
        return

    target_username = parts[1].strip()
    message_text    = parts[2].strip()

    user = db_get_user_by_username(target_username)

    if not user:
        bot.send_message(msg.chat.id, f"❌ User <b>{target_username}</b> not found.", parse_mode="HTML")
        return

    try:
        bot.send_message(user["user_id"], message_text, parse_mode="HTML")
        bot.send_message(
            msg.chat.id,
            f"✅ Message sent to <b>{fmt_name(user)}</b> (<code>{user['user_id']}</code>).",
            parse_mode="HTML",
        )
        logger.info("Admin sent message to user %s.", user["user_id"])
    except Exception as exc:
        bot.send_message(
            msg.chat.id,
            f"❌ Failed to send message to <b>{fmt_name(user)}</b>.\nError: {exc}",
            parse_mode="HTML",
        )
        logger.warning("Failed to send admin message to user %s: %s", user["user_id"], exc)


# ─────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    logger.info("Bot starting...")
    bot.infinity_polling(timeout=30, long_polling_timeout=20)
