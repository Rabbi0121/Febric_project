from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# Support direct execution (e.g., `python fabric_project/integrations/telegram_bot.py`)
# where package imports would otherwise fail without PYTHONPATH.
if __package__ in {None, ""}:
    _PROJECT_ROOT = Path(__file__).resolve().parents[2]
    if str(_PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(_PROJECT_ROOT))

from fabric_project.common.logging import configure_logging
from fabric_project.common.settings import load_settings
from fabric_project.integrations.bot_shared import run_quality_report_for_bot

logger = logging.getLogger(__name__)



def _authorized_chat(chat_id: int) -> bool:
    settings = load_settings()
    if not settings.telegram_allowed_chat_ids:
        return True
    return chat_id in settings.telegram_allowed_chat_ids


async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return
    if not _authorized_chat(update.effective_chat.id):
        return
    await update.message.reply_text(
        "Use /dqreport to run Great Expectations checks and get a failure summary."
    )


async def dqreport_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return
    if not _authorized_chat(update.effective_chat.id):
        return

    await update.message.reply_text("Running data quality checks. This can take a minute.")

    try:
        response = await asyncio.to_thread(run_quality_report_for_bot)
    except Exception as exc:  # pragma: no cover - runtime guard
        logger.exception("Failed to run quality checks from Telegram")
        await update.message.reply_text(f"Quality check failed: {type(exc).__name__}: {exc}")
        return

    await update.message.reply_text(response.summary)

    if response.markdown_report.exists():
        with response.markdown_report.open("rb") as doc:
            await update.message.reply_document(document=doc)



def main() -> None:
    configure_logging()
    settings = load_settings()
    if not settings.telegram_bot_token:
        logger.error(
            "TELEGRAM_BOT_TOKEN is required. Set it in .env or environment variables."
        )
        raise SystemExit(2)

    app = ApplicationBuilder().token(settings.telegram_bot_token).build()
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(CommandHandler("dqreport", dqreport_handler))

    logger.info("Starting Telegram bot")
    app.run_polling()


if __name__ == "__main__":
    main()
