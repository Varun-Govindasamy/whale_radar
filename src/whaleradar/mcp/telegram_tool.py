"""MCP Tool — Telegram Bot for sending whale alerts."""

from __future__ import annotations

from langchain_core.tools import tool
from telegram import Bot

from whaleradar.config import settings

_bot: Bot | None = None


def _get_bot() -> Bot:
    global _bot
    if _bot is None:
        _bot = Bot(token=settings.telegram_bot_token)
    return _bot


@tool
async def send_telegram_alert(message: str) -> str:
    """Send a whale alert message to the configured Telegram chat.

    Args:
        message: The formatted alert text to send.

    Returns:
        Confirmation string with message ID.
    """
    bot = _get_bot()
    result = await bot.send_message(
        chat_id=settings.telegram_chat_id,
        text=message,
        parse_mode="Markdown",
    )
    return f"Telegram alert sent (message_id={result.message_id})"


async def send_alert_direct(message: str) -> None:
    """Direct send without LangChain tool wrapper — used by orchestrator."""
    bot = _get_bot()
    # Truncate if too long for Telegram (4096 char limit)
    if len(message) > 4000:
        message = message[:3997] + "..."
    await bot.send_message(
        chat_id=settings.telegram_chat_id,
        text=message,
        parse_mode="Markdown",
    )
    print("[telegram] Alert sent.")
