"""
Global weather: Normal / Sunshine Sunflowers / Jerm Cloud.

Procced as a `weather_proc` outcome in duck_manager.DUCK_OUTCOME_WEIGHTS.
When it procs, a new weather is picked uniformly from the two non-current weathers.
"""

from __future__ import annotations

import random
from pathlib import Path

import discord

from .paths import ASSETS_DIR

WEATHER_DIR = ASSETS_DIR / "weather"

NORMAL = "normal"
SUNSHINE_SUNFLOWERS = "sunshine_sunflowers"
JERM_CLOUD = "jerm_cloud"

ALL_WEATHERS = (NORMAL, SUNSHINE_SUNFLOWERS, JERM_CLOUD)

# Sidebar color for every weather-related embed (procs and `!weathers`).
WEATHER_EMBED_COLOR = discord.Color.blurple()

# Short labels for catch cards (`!duck` embeds).
WEATHER_CATCH_LABELS: dict[str, str] = {
    NORMAL: "Normal",
    SUNSHINE_SUNFLOWERS: "Sunshine Sunflowers",
    JERM_CLOUD: "Jerm Cloud",
}


def weather_catch_label(weather_id: str) -> str:
    return WEATHER_CATCH_LABELS.get(weather_id, weather_id.replace("_", " ").title())


_WEATHER_COPY = {
    NORMAL: (
        "Normal weather has been activated",
        "Oh... that's... boring. Nothing is different.",
    ),
    SUNSHINE_SUNFLOWERS: (
        "Ashley brings good weather",
        "Ashley's Sunny Sunflowers allows for higher chances of shinies to be caught.",
    ),
    JERM_CLOUD: (
        "Jerm brings in terrible weather",
        "Jerm and his many clones bring in terrible weather, increasing all catch cooldowns by 50%.",
    ),
}


class WeatherManager:
    """Global weather state; instance is shared across all users via the DuckManager cog."""

    __slots__ = ("_current",)

    def __init__(self) -> None:
        self._current: str = NORMAL

    @property
    def current(self) -> str:
        return self._current

    def pick_new(self) -> str:
        """Roll a new weather uniformly from the two weathers that aren't current. Sets + returns it."""
        choices = [w for w in ALL_WEATHERS if w != self._current]
        new_weather = random.choice(choices)
        self._current = new_weather
        return new_weather

    def shiny_multiplier(self) -> float:
        return 3.0 if self._current == SUNSHINE_SUNFLOWERS else 1.0

    def cooldown_multiplier(self) -> float:
        return 1.5 if self._current == JERM_CLOUD else 1.0

    @staticmethod
    def build_proc_embed(weather_id: str) -> tuple[discord.Embed, discord.File | None]:
        title, description = _WEATHER_COPY[weather_id]
        embed = discord.Embed(title=title, description=description, color=WEATHER_EMBED_COLOR)
        asset_path: Path = WEATHER_DIR / f"{weather_id}.png"
        file_obj: discord.File | None = None
        if asset_path.is_file():
            filename = f"weather_{weather_id}.png"
            file_obj = discord.File(asset_path, filename=filename)
            embed.set_image(url=f"attachment://{filename}")
        return embed, file_obj
