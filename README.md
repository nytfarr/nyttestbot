# Refer-to-Earn Telegram Bot

## Stack
- pyTelegramBotAPI
- PostgreSQL (Supabase)
- psycopg2-binary (connection pooling)

## Deploy on Railway

1. Push this repo to GitHub
2. Create a new Railway project → Deploy from GitHub
3. Set environment variables in Railway dashboard:
   - `BOT_TOKEN`
   - `OWNER_ID`
   - `BOT_USERNAME`
   - `DATABASE_URL`
4. Railway will auto-detect `Procfile` and run `python main.py`

## Local Development

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in your values
python main.py

