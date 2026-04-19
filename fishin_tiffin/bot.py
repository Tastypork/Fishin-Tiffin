from __future__ import annotations

import logging
from typing import Any

import aiohttp
import discord
import yaml
from discord.ext import commands

from .duck_manager import DuckManager, try_consume_duck_typo
from .paths import REPO_ROOT

CONFIG_FILE = REPO_ROOT / "config.yml"
DEFAULT_DUCK_GAME_API_URL = "https://api.duckgame.app"
DEFAULT_DUCK_DASHBOARD_BASE_URL = "https://api.duckgame.app/user"
DEFAULT_LEADERBOARD_SEASON = "Preseason"

REQUIRED_KEYS = ("token", "ducks_channel")
INT_KEYS = {
    "ducks_channel": "channel ID",
    "server": "guild ID",
    "roles_channel": "channel ID",
    "duck_role": "role ID",
}
STR_KEYS = ("duck_game_api_url", "duck_dashboard_base_url", "duck_game_api_shared_secret")


def _load_config() -> dict[str, Any]:
    if not CONFIG_FILE.exists():
        raise RuntimeError(
            "Missing config.yml. Create it with 'token', 'ducks_channel', and 'duck_game_api_url'."
        )
    with CONFIG_FILE.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    missing = [key for key in REQUIRED_KEYS if not config.get(key)]
    if missing:
        raise RuntimeError(f"Missing required config keys: {', '.join(missing)}")

    for key, label in INT_KEYS.items():
        value = config.get(key)
        if value is None:
            continue
        try:
            int(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Config key '{key}' must be a valid integer {label}.") from exc

    for key in STR_KEYS:
        value = config.get(key)
        if value is not None and (not isinstance(value, str) or not value.strip()):
            raise RuntimeError(f"Config key '{key}' must be a non-empty string when set.")

    return config


def _int_or_none(value: Any) -> int | None:
    return int(value) if value is not None else None


class FishinTiffin(commands.Bot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        config = _load_config()
        self.token: str = config["token"]
        self.server: int | None = _int_or_none(config.get("server"))
        self.ducks: int = int(config["ducks_channel"])
        self.roles_channel: int | None = _int_or_none(config.get("roles_channel"))
        self.duck_role: int | None = _int_or_none(config.get("duck_role"))
        self.duck_game_api_url: str = (
            config.get("duck_game_api_url") or DEFAULT_DUCK_GAME_API_URL
        ).rstrip("/")
        self.duck_dashboard_base_url: str = (
            config.get("duck_dashboard_base_url") or DEFAULT_DUCK_DASHBOARD_BASE_URL
        ).rstrip("/")
        season = config.get("leaderboard_season")
        self.leaderboard_season: str = (
            season.strip() if isinstance(season, str) and season.strip() else DEFAULT_LEADERBOARD_SEASON
        )
        self.duck_game_api_shared_secret: str | None = config.get("duck_game_api_shared_secret") or None
        self.http_session: aiohttp.ClientSession | None = None

    async def setup_hook(self) -> None:
        self.http_session = aiohttp.ClientSession()
        await self.add_cog(DuckManager(self))

    async def close(self) -> None:
        if self.http_session:
            await self.http_session.close()
            self.http_session = None
        await super().close()

    async def on_message(self, message: discord.Message) -> None:
        if await try_consume_duck_typo(self, message):
            return
        await self.process_commands(message)


def run_bot() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    bot = FishinTiffin(
        command_prefix="!",
        intents=discord.Intents.all(),
        help_command=None,
        case_insensitive=True,
    )
    bot.run(bot.token)
