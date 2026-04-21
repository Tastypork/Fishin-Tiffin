"""
Zay's ENERGY — up to three sequential !duck defenses; first failure ends the event.
"""

from __future__ import annotations

import logging
import math
import random
import sqlite3
from pathlib import Path

import discord

from .paths import ASSETS_DIR

LOGGER = logging.getLogger("fishin_tiffin.ducks")

# Proc frequency: see duck_manager.DUCK_OUTCOME_WEIGHTS ("zay_proc", 1 of 100).
# Per-round defense success chance (rounds 1–3); first failed roll ends in a steal finale.
ZAY_DEFENSE_PROBS = (0.75, 0.5, 0.25)

ZAY_ENERGY_DIR = ASSETS_DIR / "zay_energy"
CLASH_ASSET_PATH = ZAY_ENERGY_DIR / "clash.png"
ZAY_ENERGY_GIF_PATH = ZAY_ENERGY_DIR / "zay_energy.gif"
ZAY_FINALE_WHEN_STOLE_ASSET_PATH = ZAY_ENERGY_DIR / "zay_success.png"
ZAY_FINALE_WHEN_DEFENDED_ASSET_PATH = ZAY_ENERGY_DIR / "zay_defeat.png"

ZAY_PROC_TITLE = "ZAY IS COMING FOR YOUR FLOCK"

ZAY_MID_DEFENSE_TITLE = "Good defense! He's coming in for another attack"

ZAY_STEAL_FINALE_TITLE = "Zay stole from your flock…"
ZAY_FULL_DEFENSE_TITLE = "You drove him off!"


def zay_proc_description() -> str:
    return (
        "Zay is trying to snatch birds from your collection. Use `!duck` to fight him off."
    )


def zay_proc_footer() -> str:
    return "He's picking targets from ducks he can actually take — stand your ground with `!duck`."


def zay_mid_defense_description() -> str:
    return (
        "You protected some of your ducks but he's still lurking, use `!duck` again to fend him off!"
    )


def zay_mid_defense_footer() -> str:
    return "You got this, `!duck` to protect your ducks again!"


def _steal_budget_for_round(round_index: int, snapshot_n: int) -> int:
    """round_index is 1..3; snapshot_n is loss-eligible count at event start."""
    if round_index == 1:
        return 6 + math.ceil(0.01 * snapshot_n)
    if round_index == 2:
        return 3 + math.ceil(0.005 * snapshot_n)
    return 1 + math.ceil(0.0025 * snapshot_n)


def _is_loss_eligible(duck_row: sqlite3.Row | None) -> bool:
    if duck_row is None:
        return False
    return duck_row["rarity"] not in ("Legendary", "Mythic")


class ZayEnergy:
    """In-memory Zay encounter: three ordered defense rolls, no background tasks."""

    __slots__ = ("_bot", "_cog", "_channel", "_next_round", "_snapshot_n")

    def __init__(self, bot: discord.Client, duck_cog: object) -> None:
        self._bot = bot
        self._cog = duck_cog
        self._channel: dict[str, tuple[int, int]] = {}
        self._next_round: dict[str, int] = {}
        self._snapshot_n: dict[str, int] = {}

    def active(self, user_id: str) -> bool:
        return user_id in self._channel

    def _clear_user(self, user_id: str) -> None:
        self._channel.pop(user_id, None)
        self._next_round.pop(user_id, None)
        self._snapshot_n.pop(user_id, None)

    def _loss_eligible_duck_ids(self, user_id: str) -> list[str]:
        out: list[str] = []
        get_ids = getattr(self._cog, "_get_user_duck_ids")
        get_duck = getattr(self._cog, "_get_duck")
        for duck_id in get_ids(user_id):
            row = get_duck(duck_id)
            if row and _is_loss_eligible(row):
                out.append(duck_id)
        return out

    def start(self, user_id: str, guild_id: int, channel_id: int) -> None:
        self._clear_user(user_id)
        self._snapshot_n[user_id] = len(self._loss_eligible_duck_ids(user_id))
        self._next_round[user_id] = 1
        self._channel[user_id] = (guild_id, channel_id)

    def cancel_all_tasks(self) -> None:
        """Clear every active encounter. Name kept for duck_manager.cog_unload compatibility."""
        for uid in list(self._channel.keys()):
            self._clear_user(uid)

    def _resolve_channel(self, guild_id: int, channel_id: int) -> discord.abc.Messageable | None:
        ch = self._bot.get_channel(channel_id)
        if ch is not None and isinstance(ch, discord.abc.Messageable):
            return ch
        guild = self._bot.get_guild(guild_id)
        if guild is None:
            return None
        fallback = guild.get_channel(channel_id)
        return fallback if isinstance(fallback, discord.abc.Messageable) else None

    @staticmethod
    def _mention_for_user(channel: discord.abc.Messageable, user_id: str) -> str:
        try:
            guild = getattr(channel, "guild", None)
            if guild is not None:
                member = guild.get_member(int(user_id))
                if member is not None:
                    return member.mention
        except Exception:
            pass
        return f"<@{user_id}>"

    @staticmethod
    async def _send_finale(
        channel: discord.abc.Messageable,
        embed: discord.Embed,
        image_path: Path,
        attachment_filename: str,
        log_tag: str,
    ) -> None:
        file_obj: discord.File | None = None
        if image_path.is_file():
            file_obj = discord.File(image_path, filename=attachment_filename)
            embed.set_image(url=f"attachment://{attachment_filename}")
        try:
            if file_obj is not None:
                await channel.send(embed=embed, file=file_obj)
            else:
                await channel.send(embed=embed)
        except discord.HTTPException as e:
            LOGGER.info("[zay] %s finale send failed: %s", log_tag, e)

    async def handle_defense_attempt(self, ctx: commands.Context, user_id: str) -> None:
        """One `!duck` resolves the current round: steal finale, full-defense finale, or clash reply."""
        if user_id not in self._channel:
            return

        rnd = self._next_round[user_id]
        if rnd not in (1, 2, 3):
            LOGGER.warning("[zay] invalid round %s for user %s; clearing state", rnd, user_id)
            self._clear_user(user_id)
            return

        snapshot_n = self._snapshot_n[user_id]
        defend_p = ZAY_DEFENSE_PROBS[rnd - 1]
        defended = random.random() < defend_p

        if not defended:
            await self._finish_steal(user_id, rnd, snapshot_n)
            return

        if rnd >= 3:
            await self._finish_full_defense(user_id)
            return

        self._next_round[user_id] = rnd + 1
        embed = discord.Embed(
            title=ZAY_MID_DEFENSE_TITLE,
            description=zay_mid_defense_description(),
            color=discord.Color.orange(),
        )
        clash_file = None
        if CLASH_ASSET_PATH.is_file():
            clash_file = discord.File(CLASH_ASSET_PATH, filename="clash.png")
            embed.set_image(url="attachment://clash.png")
        embed.set_footer(text=zay_mid_defense_footer())
        try:
            if clash_file is not None:
                await ctx.reply(embed=embed, file=clash_file, mention_author=False)
            else:
                await ctx.reply(embed=embed, mention_author=False)
        except discord.HTTPException as e:
            LOGGER.info("[zay] mid-defense reply failed: %s", e)

    async def _finish_steal(
        self,
        user_id: str,
        failed_round: int,
        snapshot_n: int,
    ) -> None:
        guild_id, channel_id = self._channel[user_id]
        budget = _steal_budget_for_round(failed_round, snapshot_n)
        eligible = self._loss_eligible_duck_ids(user_id)
        remove_n = min(budget, len(eligible))
        lost_names: list[str] = []
        obliterate = getattr(self._cog, "_obliterate_duck")
        get_duck = getattr(self._cog, "_get_duck")
        if remove_n > 0:
            random.shuffle(eligible)
            for duck_id in eligible[:remove_n]:
                row = get_duck(duck_id)
                if row:
                    lost_names.append(str(row["name"]))
                obliterate(user_id, duck_id)

        self._clear_user(user_id)

        channel = self._resolve_channel(guild_id, channel_id)
        if channel is None:
            return

        mention = self._mention_for_user(channel, user_id)
        names_bit = ""
        if lost_names:
            preview = ", ".join(lost_names[:8])
            if len(lost_names) > 8:
                preview += f", +{len(lost_names) - 8} more"
            names_bit = f"\n\n**Taken:** {preview}"

        if remove_n > 0:
            desc = (
                f"{mention} **While he was stealing, you couldn't stop every grab.** "
                f"**{remove_n}** duck{'s' if remove_n != 1 else ''} are gone.{names_bit}"
            )
        else:
            desc = (
                f"{mention} **Zay came for your flock, but nothing he could take was on the table.** "
                "**Legendary** and **Mythic** ducks are untouchable."
            )

        embed = discord.Embed(
            title=ZAY_STEAL_FINALE_TITLE,
            description=desc,
            color=discord.Color.dark_red() if remove_n > 0 else discord.Color.green(),
        )
        embed.set_footer(text="You go back to strengthen your defenses")
        await self._send_finale(
            channel,
            embed,
            ZAY_FINALE_WHEN_STOLE_ASSET_PATH,
            "zay_success.png",
            "steal",
        )

    async def _finish_full_defense(self, user_id: str) -> None:
        guild_id, channel_id = self._channel[user_id]
        snapshot_n = self._snapshot_n[user_id]
        self._clear_user(user_id)

        channel = self._resolve_channel(guild_id, channel_id)
        if channel is None:
            return

        mention = self._mention_for_user(channel, user_id)
        if snapshot_n == 0:
            desc = (
                f"{mention} **Zay couldn't steal any of your birds.** "
                "**Legendary** and **Mythic** ducks are untouchable — nothing else was on the table either."
            )
        else:
            desc = f"{mention} **Zay couldn't steal any of your birds.**"
        embed = discord.Embed(
            title=ZAY_FULL_DEFENSE_TITLE,
            description=desc,
            color=discord.Color.green(),
        )
        embed.set_footer(text="Your flock's safe — for now.")
        await self._send_finale(
            channel,
            embed,
            ZAY_FINALE_WHEN_DEFENDED_ASSET_PATH,
            "zay_defeat.png",
            "full-defense",
        )
