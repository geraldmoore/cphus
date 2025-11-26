import discord

from src.cphus.core.config.logging import get_logger
from src.cphus.core.config.settings import get_settings

logger = get_logger(__name__)


class DiscordMessenger:
    def __init__(self):
        """Initialise Discord messenger."""
        self.settings = get_settings()

    async def send_message(self, message: str):
        """Send a message to a channel.

        Args:
            message (str): Message to send.
        """
        intents = discord.Intents.default()
        client = discord.Client(intents=intents)

        @client.event
        async def on_ready():
            channel = client.get_channel(self.settings.discord_channel_id)

            if channel:
                await channel.send(message)
                logger.info("Message sent!")
            else:
                logger.info("Channel not found!")

            await client.close()

        await client.start(self.settings.discord_bot_token.get_secret_value())
