from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, ContextTypes, filters
from app.rag_engine import recommend_jobs
import os
from dotenv import load_dotenv

load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")  # из .env

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_query = update.message.text
    await update.message.reply_text("🔍 Ищу релевантные вакансии...")

    try:
        response = recommend_jobs(user_query)
        await update.message.reply_text(f"📌 Вот что удалось найти:\n\n{response}")
    except Exception as e:
        await update.message.reply_text(f"⚠️ Ошибка: {str(e)}")

if __name__ == '__main__':
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.run_polling()
