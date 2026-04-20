from __future__ import annotations

import logging

import discord
import yaml
from discord.ext import commands

from .duck_manager import DuckManager, try_consume_duck_typo
from .paths import REPO_ROOT

CONFIG_FILE = REPO_ROOT / "config.yml"
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_DUCK_API_URL = "https://duck.jocal.dev/duck"
DEFAULT_DUCK_DASHBOARD_BASE_URL = "https://duck.jocal.dev/user"


class FishinTiffin(commands.Bot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = self._load_config()
        self.token = self.config["token"]
        self.server = int(self.config["server"]) if self.config.get("server") else None
        self.ducks = int(self.config["ducks_channel"])
        self.roles_channel = int(self.config["roles_channel"]) if self.config.get("roles_channel") else None
        self.duck_role = int(self.config["duck_role"]) if self.config.get("duck_role") else None
        self.duck_api_url = (self.config.get("duck_api_url") or DEFAULT_DUCK_API_URL).rstrip("/")
        self.duck_dashboard_base_url = (
            self.config.get("duck_dashboard_base_url") or DEFAULT_DUCK_DASHBOARD_BASE_URL
        ).rstrip("/")
        self.log_level = DEFAULT_LOG_LEVEL

    async def setup_hook(self) -> None:
        await self.add_cog(DuckManager(self, duck_api_url=self.duck_api_url))

    async def on_message(self, message: discord.Message) -> None:
        if await try_consume_duck_typo(self, message):
            return
        await self.process_commands(message)

    @staticmethod
    def _load_config() -> dict:
        if not CONFIG_FILE.exists():
            raise RuntimeError(
                "Missing config.yml. Create it with 'token' and 'ducks_channel'."
            )
        with CONFIG_FILE.open("r", encoding="utf-8") as config_file:
            config = yaml.safe_load(config_file) or {}

        required_keys = ["token", "ducks_channel"]
        missing = [key for key in required_keys if not config.get(key)]
        if missing:
            raise RuntimeError(f"Missing required config keys: {', '.join(missing)}")

        try:
            int(config["ducks_channel"])
        except (TypeError, ValueError) as exc:
            raise RuntimeError("Config key 'ducks_channel' must be a valid integer channel ID.") from exc

        if config.get("server") is not None:
            try:
                int(config["server"])
            except (TypeError, ValueError) as exc:
                raise RuntimeError("Config key 'server' must be a valid integer guild ID.") from exc

        if config.get("roles_channel") is not None:
            try:
                int(config["roles_channel"])
            except (TypeError, ValueError) as exc:
                raise RuntimeError("Config key 'roles_channel' must be a valid integer channel ID.") from exc

        if config.get("duck_role") is not None:
            try:
                int(config["duck_role"])
            except (TypeError, ValueError) as exc:
                raise RuntimeError("Config key 'duck_role' must be a valid integer role ID.") from exc

        for url_key in ("duck_api_url", "duck_dashboard_base_url"):
            val = config.get(url_key)
            if val is not None and (not isinstance(val, str) or not val.strip()):
                raise RuntimeError(
                    f"Config key '{url_key}' must be a non-empty string when set."
                )

        return config


def run_bot() -> None:
    intents = discord.Intents.all()
    bot = FishinTiffin(
        command_prefix="!", intents=intents, help_command=None, case_insensitive=True
    )

    logging.basicConfig(
        level=getattr(logging, str(bot.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    bot.run(bot.token)

