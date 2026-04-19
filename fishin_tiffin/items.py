"""
Per-user item effects: Big Guy Protein (battle strength) and AriPie Energy (cooldown reduction).

Procced as an `item_proc` outcome in duck_manager.DUCK_OUTCOME_WEIGHTS. When it procs,
one item is granted uniformly. Active effects auto-expire; a re-proc refreshes expiry.
"""

from __future__ import annotations

import random
from pathlib import Path

import discord

from .duck_clock import utc_ts
from .paths import ASSETS_DIR

ITEMS_DIR = ASSETS_DIR / "items"

BIG_GUY = "big_guy"
ARI_PIE = "ari_pie"

ALL_ITEMS = (BIG_GUY, ARI_PIE)

_DURATIONS = {
    BIG_GUY: 2 * 3600,
    ARI_PIE: 30 * 60,
}

BIG_GUY_STAT_BONUS = 2
ARI_PIE_COOLDOWN_DIVISOR = 5

_ITEM_COPY = {
    BIG_GUY: (
        "Josh gives you Big Guy Protein!",
        f"Big Guy Protein gives extra strength (+{BIG_GUY_STAT_BONUS} to each stat) to your ducks in battle for the next 2 hours.",
        discord.Color.red(),
    ),
    ARI_PIE: (
        "Ari gives you her signature drink!",
        f"AriPie Energy reduces your cooldowns by {ARI_PIE_COOLDOWN_DIVISOR}x for the next 30 minutes.",
        discord.Color.magenta(),
    ),
}

# Display names for active item lines on `!duck` catch cards.
_ITEM_CATCH_LABELS: dict[str, str] = {
    BIG_GUY: "Big Guy Protein",
    ARI_PIE: "AriPie Energy",
}


class ItemsManager:
    """Tracks per-user active item effects with expiry timestamps."""

    __slots__ = ("_expiry",)

    def __init__(self) -> None:
        self._expiry: dict[str, dict[str, int]] = {}

    def _active(self, user_id: str, item_id: str) -> bool:
        item_map = self._expiry.get(user_id)
        if not item_map:
            return False
        until = item_map.get(item_id)
        if until is None:
            return False
        if utc_ts() >= until:
            del item_map[item_id]
            if not item_map:
                del self._expiry[user_id]
            return False
        return True

    def grant_random(self, user_id: str) -> str:
        """Pick uniformly between items, set/refresh expiry, return the item id."""
        item_id = random.choice(ALL_ITEMS)
        until = utc_ts() + _DURATIONS[item_id]
        self._expiry.setdefault(user_id, {})[item_id] = until
        return item_id

    def battle_bonus(self, user_id: str) -> int:
        return BIG_GUY_STAT_BONUS if self._active(user_id, BIG_GUY) else 0

    def cooldown_divisor(self, user_id: str) -> int:
        return ARI_PIE_COOLDOWN_DIVISOR if self._active(user_id, ARI_PIE) else 1

    def active_item_effect_labels(self, user_id: str) -> list[str]:
        """Human-readable names of item effects currently active on this user (stable order)."""
        return [_ITEM_CATCH_LABELS[i] for i in ALL_ITEMS if self._active(user_id, i)]

    @staticmethod
    def build_proc_embed(item_id: str) -> tuple[discord.Embed, discord.File | None]:
        title, description, color = _ITEM_COPY[item_id]
        embed = discord.Embed(title=title, description=description, color=color)
        asset_path: Path = ITEMS_DIR / f"{item_id}.png"
        file_obj: discord.File | None = None
        if asset_path.is_file():
            filename = f"item_{item_id}.png"
            file_obj = discord.File(asset_path, filename=filename)
            embed.set_image(url=f"attachment://{filename}")
        return embed, file_obj
