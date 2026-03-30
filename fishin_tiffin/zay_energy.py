"""
Zay's ENERGY — defense event with backend tally and finale resolution.
"""

from __future__ import annotations

import asyncio
import logging
import random
import sqlite3
import discord

from .duck_clock import utc_ts
from .keish_energy import BLESSING_DISPLAY_SECONDS, BLESSING_DURATION_SECONDS
from .paths import ASSETS_DIR

LOGGER = logging.getLogger("fishin_tiffin.ducks")

ZAY_ENERGY_PROB = 0.01
ZAY_DURATION_SECONDS = BLESSING_DURATION_SECONDS
ZAY_DISPLAY_SECONDS = BLESSING_DISPLAY_SECONDS
ZAY_STEAL_TOTAL_MIN = 15
ZAY_STEAL_TOTAL_MAX = 35

CLASH_ASSET_PATH = ASSETS_DIR / "clash.png"
ZAY_ENERGY_GIF_PATH = ASSETS_DIR / "zay_energy.gif"
# Finale: file names match art — steal outcome vs defended outcome
ZAY_FINALE_WHEN_STOLE_ASSET_PATH = ASSETS_DIR / "zay_success.png"  # Zay got ducks
ZAY_FINALE_WHEN_DEFENDED_ASSET_PATH = ASSETS_DIR / "zay_defeat.png"  # no ducks lost

ZAY_PROC_TITLE = "ZAY IS STEALING YOUR DUCKS — USE `!DUCK` TO DEFEND!!!"
ZAY_SNAG_TITLE = "🌑 New duck—but **Zay is stealing your flock!** **`!duck`** to defend!"
ZAY_SADDLE_TITLE = "🌑✨ Shiny secured—**Zay's still stealing your ducks!** **`!duck`** to fight back!"

DEFENSE_EMBED_TITLE = "⚔️ You fought him off!"


def zay_proc_description() -> str:
    return (
        "**Zay is actively stealing your ducks.** "
        f"For **{ZAY_DISPLAY_SECONDS}** seconds, **`!duck` has no cooldown**—"
        "**use `!duck`** again and again to **defend** and stop him from walking out with your birds. "
        "He **cannot** take **Legendary** or **Mythic** ducks."
    )


def defense_embed_body(protected: int) -> str:
    return (
        f"**You used `!duck`** and kept **{protected}** ducks out of his reach—"
        "**Zay is still stealing from your flock.** Don't stop now."
    )


def defense_embed_footer(rem_disp: int) -> str:
    return (
        f"🌑 **Zay is actively stealing your ducks**—**{rem_disp}s** left. "
        "**Use `!duck`** to keep defending."
    )


def cd_message_proc() -> str:
    return (
        f"🌑 **Zay is actively stealing your ducks** for **{ZAY_DISPLAY_SECONDS}** seconds. "
        "**No `!duck` cooldown—use `!duck`** over and over to defend your flock."
    )


def cd_message_active(rem_disp: int) -> str:
    return (
        f"🌑 **Zay is still stealing your ducks**—**{rem_disp}s** left. "
        "**No `!duck` cooldown—keep using `!duck`** to defend."
    )


def _is_loss_eligible(duck_row: sqlite3.Row | None) -> bool:
    if duck_row is None:
        return False
    return duck_row["rarity"] not in ("Legendary", "Mythic")


class ZayEnergy:
    """Runtime state and asyncio task for Zay's Energy."""

    __slots__ = ("_bot", "_cog", "_until", "_stolen", "_protected", "_tasks", "_channel")

    def __init__(self, bot: discord.Client, duck_cog: object) -> None:
        self._bot = bot
        self._cog = duck_cog
        self._until: dict[str, int] = {}
        self._stolen: dict[str, int] = {}
        self._protected: dict[str, int] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._channel: dict[str, tuple[int, int]] = {}

    def active(self, user_id: str) -> bool:
        until = self._until.get(user_id)
        if until is None:
            return False
        return utc_ts() < until

    def _remaining(self, user_id: str) -> int:
        until = self._until.get(user_id)
        if until is None:
            return 0
        return max(0, until - utc_ts())

    def remaining_display(self, user_id: str) -> int:
        return self._remaining(user_id) // 2

    def add_protected(self, user_id: str, delta: int) -> None:
        self._protected[user_id] = self._protected.get(user_id, 0) + delta

    def _clear_user(self, user_id: str, cancel_task: bool = True) -> None:
        self._until.pop(user_id, None)
        self._stolen.pop(user_id, None)
        self._protected.pop(user_id, None)
        self._channel.pop(user_id, None)
        if cancel_task:
            t = self._tasks.pop(user_id, None)
            if t and not t.done():
                t.cancel()

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
        self._clear_user(user_id, cancel_task=True)
        now = utc_ts()
        self._until[user_id] = now + ZAY_DURATION_SECONDS
        self._stolen[user_id] = 0
        self._protected[user_id] = 0
        self._channel[user_id] = (guild_id, channel_id)
        task = asyncio.create_task(self._run_steal_ticks(user_id))
        self._tasks[user_id] = task

    async def _run_steal_ticks(self, user_id: str) -> None:
        try:
            await asyncio.sleep(ZAY_DURATION_SECONDS)
            if user_id not in self._until:
                return
        except asyncio.CancelledError:
            self._tasks.pop(user_id, None)
            self._clear_user(user_id, cancel_task=False)
            raise
        await self._resolve(user_id)

    async def _resolve(self, user_id: str) -> None:
        if user_id not in self._channel:
            self._tasks.pop(user_id, None)
            self._clear_user(user_id, cancel_task=False)
            return

        guild_id, channel_id = self._channel[user_id]
        # Single-roll final steal budget for the full event.
        stolen = random.randint(ZAY_STEAL_TOTAL_MIN, ZAY_STEAL_TOTAL_MAX)
        self._stolen[user_id] = stolen
        protected = self._protected.get(user_id, 0)
        self._until.pop(user_id, None)
        net = max(0, stolen - protected)

        eligible = self._loss_eligible_duck_ids(user_id)
        remove_n = min(net, len(eligible))
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

        self._tasks.pop(user_id, None)
        self._clear_user(user_id, cancel_task=False)

        channel = self._bot.get_channel(channel_id)
        if channel is None:
            try:
                guild = self._bot.get_guild(guild_id)
                if guild:
                    channel = guild.get_channel(channel_id)
            except Exception:
                pass
        if channel is None:
            return

        try:
            member = channel.guild.get_member(int(user_id)) if channel.guild else None
            mention = member.mention if member else f"<@{user_id}>"
        except Exception:
            mention = f"<@{user_id}>"

        if remove_n == 0:
            title = "You drove him off!"
            if net == 0:
                desc = (
                    f"{mention} **Your `!duck` defense worked—Zay didn't get your birds.** "
                    "**Nobody lost a duck.**"
                )
            else:
                desc = (
                    f"{mention} **Zay was stealing**, but nothing he could take was on the table—"
                    "**Legendary** and **Mythic** ducks are untouchable. **You're safe.**"
                )
            asset_path = ZAY_FINALE_WHEN_DEFENDED_ASSET_PATH
            attach_name = "zay_defeat.png"
        else:
            title = "Zay stole from your flock…"
            names_bit = ""
            if lost_names:
                preview = ", ".join(lost_names[:8])
                if len(lost_names) > 8:
                    preview += f", +{len(lost_names) - 8} more"
                names_bit = f"\n\n**Taken:** {preview}"
            desc = (
                f"{mention} **While he was stealing, you couldn't stop every grab.** "
                f"**{remove_n}** duck{'s' if remove_n != 1 else ''} are gone.{names_bit}"
            )
            asset_path = ZAY_FINALE_WHEN_STOLE_ASSET_PATH
            attach_name = "zay_success.png"

        embed = discord.Embed(
            title=title,
            description=desc,
            color=discord.Color.dark_red() if remove_n > 0 else discord.Color.green(),
        )
        finale_file = None
        if asset_path.is_file():
            finale_file = discord.File(asset_path, filename=attach_name)
            embed.set_image(url=f"attachment://{attach_name}")
        try:
            if finale_file:
                await channel.send(embed=embed, file=finale_file)
            else:
                await channel.send(embed=embed)
        except discord.HTTPException as e:
            LOGGER.info("[zay] finale send failed: %s", e)

    def cancel_all_tasks(self) -> None:
        for t in list(self._tasks.values()):
            t.cancel()
        self._tasks.clear()
