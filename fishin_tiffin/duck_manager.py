"""Discord presentation layer. All game state lives in Duck-Game-backend."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import aiohttp
import discord
from discord.ext import commands, tasks

from .paths import ASSETS_DIR, DUCK_GAME_STATIC_GAME_DIR

LOGGER = logging.getLogger("fishin_tiffin.ducks")


class DuckApiError(RuntimeError):
    """HTTP error from Duck Game API with a user-facing message (FastAPI ``detail``)."""

    def __init__(self, message: str, *, status: int) -> None:
        super().__init__(message)
        self.status = status


def _parse_api_error_detail(body: str) -> str | None:
    """Extract human-readable text from FastAPI JSON error bodies."""
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return None
    d = payload.get("detail")
    if isinstance(d, str) and d.strip():
        return d.strip()
    if isinstance(d, list):
        parts: list[str] = []
        for item in d:
            if isinstance(item, dict):
                msg = item.get("msg")
                if isinstance(msg, str) and msg.strip():
                    parts.append(msg.strip())
        if parts:
            return "; ".join(parts)
    return None


DAP_GIF_PATH = ASSETS_DIR / "dap.gif"
DUCK_ROLE_EMOJI = "🦆"

TEXT_REPLY_KINDS = {
    "cooldown",
    "error",
    "revenge_closed",
    "battle_hourly_limit",
    "battle_zay_active",
    "battle_no_duck",
    "battle_no_opponent",
}

# kind -> (default_color_override, mention_author, react_crown_supported)
STATIC_ASSET_KINDS: dict[str, tuple[int | None, bool, bool]] = {
    "zay_proc": (0x2C1B47, False, False),
    "keish_proc": (0xF1C40F, False, False),
    "keish_catch": (None, False, True),
    "boot": (None, True, False),
}


def build_duck_onboarding_embed(role_mention: str) -> discord.Embed:
    return discord.Embed(
        title="Join the Duck Game",
        description=(
            f"React with {DUCK_ROLE_EMOJI} below. "
            f"React to add {role_mention} or react then remove reaction to remove the role.\n\n"
            "**How the game works**\n"
            "- Use `!duck` in the ducks channel to catch ducks.\n"
            "- Catches roll rarity and stats; some ducks are extra special.\n"
            "- `!duck` has a randomized cooldown per successful catch.\n"
            "- Sometimes your `!duck` steals one from another player.\n"
            "- **`!battle`** pits your random eligible duck against another player's; the winner takes the loser's fighter.\n"
            "- If **Zay** shows up, **`!duck`** up to **three times** to defend your flock (he leaves after a failed block).\n\n"
            "- If **Keish** shows up, your **next three** **`!duck`** commands each dump **3-7 ducks** (no cooldown between); after the third, your next **`!duck`** is a normal catch with no extra wait.\n\n"
            "**Rarities**\n"
            "- From **Common** up to **Mythic** - the higher tiers show up less often.\n\n"
            "Use `!help` anytime for commands and how things work."
        ),
        color=discord.Color.blurple(),
    )


def _levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        cur = [i]
        for j, cb in enumerate(b, start=1):
            cur.append(min(cur[j - 1] + 1, prev[j] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[-1]


def _first_bang_command_token(content: str) -> str | None:
    text = content.strip()
    if not text.startswith("!"):
        return None
    rest = text[1:].strip()
    return rest.split()[0] if rest else None


def _looks_like_duck_typo(token: str) -> bool:
    t = token.lower()
    return 3 <= len(t) <= 7 and _levenshtein(t, "duck") <= 1


async def try_consume_duck_typo(bot: commands.Bot, message: discord.Message) -> bool:
    if message.author.bot:
        return False
    ducks_ch = getattr(bot, "ducks", None)
    if ducks_ch is None or message.channel.id != ducks_ch:
        return False
    token = _first_bang_command_token(message.content)
    if not token or token.lower() in ("duck", "ducks"):
        return False
    if not _looks_like_duck_typo(token):
        return False
    cog = bot.get_cog("DuckManager")
    if cog is None:
        return False
    await cog.handle_duck_typo(message)
    return True


def _format_api_error(url: str, status: int, text: str) -> str:
    snippet = (text or "").strip().replace("\n", " ")[:400]
    hint = ""
    if status == 404:
        hint = (
            " No route here - set duck_game_api_url to your Duck-Game-backend base URL "
            "(test: curl http://HOST:PORT/ should show service duck-game-backend). "
            "If the API uses API_PREFIX=/api, base must end with /api."
        )
    return f"HTTP {status} {url} - {snippet}{hint}"


def _embed_from_payload(
    title: str | None, description: str | None, color: int | None, footer: str | None
) -> discord.Embed:
    embed = discord.Embed(
        title=title,
        description=description,
        color=discord.Color(color) if color is not None else discord.Color.blurple(),
    )
    if footer:
        embed.set_footer(text=footer)
    return embed


def _embed_from_result(result: dict, *, color_override: int | None = None) -> discord.Embed:
    color = color_override if color_override is not None else result.get("color")
    return _embed_from_payload(
        result.get("title"), result.get("description"), color, result.get("footer")
    )


def _apply_embed_image_from_api(embed: discord.Embed, image_url: str | None) -> discord.File | None:
    # Discord cannot fetch embed images from localhost / private URLs. If the API advertises a
    # /static/game/<file> path whose file exists locally (sibling backend checkout or assets/),
    # upload it and use attachment:// so GIFs/PNGs render reliably.
    if not image_url:
        return None
    name = Path(urlparse(image_url).path).name
    if not name:
        embed.set_image(url=image_url)
        return None
    for base in (DUCK_GAME_STATIC_GAME_DIR, ASSETS_DIR):
        path = base / name
        if path.is_file():
            embed.set_image(url=f"attachment://{name}")
            return discord.File(path, filename=name)
    embed.set_image(url=image_url)
    return None


def _send_kwargs(file: discord.File | None) -> dict[str, Any]:
    return {"file": file} if file is not None else {}


class _SyntheticCtx:
    """Context-like stand-in used by the announcement poller (no real invoking message)."""

    def __init__(
        self,
        bot: commands.Bot,
        channel: discord.abc.Messageable,
        guild: discord.Guild | None,
        author: discord.abc.User | discord.Member | None,
        author_id: int,
    ) -> None:
        self.bot = bot
        self.channel = channel
        self.guild = guild
        self.author = author
        self._author_id = author_id

    async def reply(self, content: str | None = None, *args, **kwargs):
        kwargs.pop("mention_author", None)
        if content is not None:
            content = f"<@{self._author_id}> {content}"
        return await self.channel.send(content, *args, **kwargs)

    async def send(self, *args, **kwargs):
        return await self.channel.send(*args, **kwargs)


class DuckManager(commands.Cog, name="DuckManager"):
    """Discord adapter for the Duck-Game-backend HTTP API."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    @property
    def api_base(self) -> str:
        return getattr(self.bot, "duck_game_api_url", "https://api.duckgame.app").rstrip("/")

    # ---- API client ---------------------------------------------------

    async def _api(
        self,
        method: str,
        path: str,
        *,
        user_id: str | None = None,
        guild_id: int | None = None,
        json_body: dict | None = None,
    ) -> dict:
        session: aiohttp.ClientSession | None = self.bot.http_session
        if session is None:
            raise RuntimeError("HTTP session is not initialised")
        url = f"{self.api_base}{path}"
        headers: dict[str, str] = {}
        if user_id is not None:
            headers["x-user-id"] = user_id
        if guild_id is not None:
            headers["x-guild-id"] = str(guild_id)

        async with session.request(method, url, json=json_body, headers=headers) as resp:
            text = await resp.text()
            if resp.status >= 400:
                user_message = _parse_api_error_detail(text)
                if user_message:
                    raise DuckApiError(user_message, status=resp.status)
                detail: Any = text
                try:
                    payload = json.loads(text)
                    detail = payload.get("detail", text)
                    if isinstance(detail, list):
                        detail = json.dumps(detail)
                except json.JSONDecodeError:
                    pass
                raise RuntimeError(_format_api_error(url, resp.status, str(detail) if detail else text))
            return json.loads(text) if text else {}

    async def _post_catch(self, user_id: str, guild_id: int | None, channel_id: int | None) -> dict:
        body: dict[str, Any] = {}
        if guild_id is not None:
            body["guild_id"] = guild_id
        if channel_id is not None:
            body["channel_id"] = channel_id
        data = await self._api("POST", "/v1/duck/catch", user_id=user_id, guild_id=guild_id, json_body=body)
        return data.get("result", data)

    async def _post_battle(self, user_id: str, guild_id: int | None) -> dict:
        body: dict[str, Any] = {}
        if guild_id is not None:
            body["guild_id"] = guild_id
        data = await self._api("POST", "/v1/duck/battle", user_id=user_id, guild_id=guild_id, json_body=body)
        return data.get("result", data)

    async def _post_give(
        self, giver_id: str, receiver_id: str, duck_name: str, guild_id: int | None = None
    ) -> dict:
        return await self._api(
            "POST",
            "/v1/ducks/give",
            user_id=giver_id,
            guild_id=guild_id,
            json_body={"receiver_id": receiver_id, "duck_name": duck_name},
        )

    async def _post_release(self, user_id: str, duck_name: str, guild_id: int | None = None) -> dict:
        return await self._api(
            "POST",
            "/v1/ducks/release",
            user_id=user_id,
            guild_id=guild_id,
            json_body={"duck_name": duck_name},
        )

    async def _get_me(self, user_id: str, guild_id: int | None = None) -> dict:
        return await self._api("GET", "/me", user_id=user_id, guild_id=guild_id)

    async def _get_leaderboard(self, guild_id: int | None = None) -> dict:
        return await self._api("GET", "/leaderboard", guild_id=guild_id)

    # ---- Dispatch -----------------------------------------------------

    async def dispatch_catch_result(self, ctx: commands.Context, result: dict) -> None:
        kind = result.get("kind")

        if kind in TEXT_REPLY_KINDS:
            await ctx.reply(result.get("message", ""), mention_author=True)
            return

        if kind in ("battle", "revenge_battle"):
            await self._send_battle_embed(ctx, result, revenge=(kind == "revenge_battle"))
            return

        if kind in STATIC_ASSET_KINDS:
            await self._send_static_asset_embed(ctx, result, kind)
            return

        if kind == "catch":
            await self._send_catch_embed(ctx, result)
            return

        if kind == "zay_effects":
            for eff in result.get("effects", []):
                await self._dispatch_zay_effect(ctx, eff)
            return

        await ctx.reply(f"Unhandled API result: `{kind}`", mention_author=True)

    async def _send_battle_embed(
        self, ctx: commands.Context, result: dict, *, revenge: bool
    ) -> None:
        if revenge:
            title = "⚔️ Revenge Duck Battle!"
            win_color = discord.Color.red() if result.get("victim_wins") else discord.Color.dark_red()
        else:
            title = "⚔️ Duck Battle!"
            win_color = discord.Color.green() if result.get("challenger_wins") else discord.Color.dark_red()
        embed = discord.Embed(
            title=title,
            description=result.get("description"),
            color=win_color,
        )
        embed.add_field(name="Outcome", value="\n".join(result.get("outcome_lines", [])), inline=False)
        embed.set_footer(text=result.get("footer", ""))
        await ctx.reply(embed=embed, mention_author=True)

    async def _send_static_asset_embed(
        self, ctx: commands.Context, result: dict, kind: str
    ) -> None:
        color_override, mention_author, supports_crown = STATIC_ASSET_KINDS[kind]
        embed = _embed_from_result(result, color_override=color_override)
        file = _apply_embed_image_from_api(embed, result.get("image_url"))
        msg = await ctx.reply(embed=embed, mention_author=mention_author, **_send_kwargs(file))
        if supports_crown and result.get("react_crown"):
            try:
                await msg.add_reaction("👑")
            except discord.HTTPException:
                pass

    async def _send_catch_embed(self, ctx: commands.Context, result: dict) -> None:
        embed = _embed_from_result(result)
        # Duck image is always a remote URL from the duck API - never a local static file.
        if result.get("image_url"):
            embed.set_image(url=result["image_url"])
        msg = await ctx.reply(embed=embed, mention_author=False)
        if result.get("react_crown"):
            try:
                await msg.add_reaction("👑")
            except discord.HTTPException:
                pass
        if result.get("theft_followup"):
            await ctx.send(result["theft_followup"])

    async def _dispatch_zay_effect(self, ctx: commands.Context, eff: dict) -> None:
        ek = eff.get("kind")
        if ek == "zay_cleared_invalid":
            return

        emb = eff.get("embed", {})
        embed = _embed_from_payload(
            emb.get("title"), emb.get("description"), emb.get("color"), emb.get("footer")
        )
        file = _apply_embed_image_from_api(embed, emb.get("image_url"))

        if ek == "zay_reply":
            await ctx.reply(embed=embed, mention_author=False, **_send_kwargs(file))
            return

        if ek == "zay_channel_broadcast":
            gid = int(eff.get("guild_id", 0))
            cid = int(eff.get("channel_id", 0))
            channel = self.bot.get_channel(cid)
            if channel is None and ctx.guild and ctx.guild.id == gid:
                channel = ctx.channel
            if channel is None or not isinstance(channel, discord.abc.Messageable):
                LOGGER.warning("zay broadcast: channel %s not found", cid)
                return
            await channel.send(embed=embed, **_send_kwargs(file))

    # ---- Commands / listeners ----------------------------------------

    async def cog_check(self, ctx: commands.Context) -> bool:
        configured = getattr(self.bot, "server", None)
        if configured is None or ctx.guild is None:
            return True
        return ctx.guild.id == configured

    async def handle_duck_typo(self, message: discord.Message) -> None:
        ctx = await self.bot.get_context(message)
        if not await self.cog_check(ctx):
            return
        content = f"{message.author.mention} dont worry fam i got you"
        dap_file = discord.File(DAP_GIF_PATH, filename="dap.gif") if DAP_GIF_PATH.is_file() else None
        kwargs = {"file": dap_file} if dap_file else {}
        await message.reply(content=content, mention_author=True, **kwargs)
        try:
            await self.duck(ctx)
        finally:
            try:
                await message.delete()
            except discord.HTTPException:
                pass

    @commands.command(name="help")
    async def duck_help(self, ctx: commands.Context):
        commands_lines = [
            "`!duck` - Catch a duck (with cooldown).",
            "`!battle` - Battle a random opponent; winner takes the loser's duck (UTC hourly limit; wins chain).",
            "`!ducks [@member]` - Open your/their duck dashboard.",
            "`!leaderboard` - Show top duck collectors.",
            "`!give @member <duck_name>` - Transfer one of your ducks.",
            "`!release <duck_name>` - Release your most recently caught duck with that name (gone forever).",
            "`!help` - Show this help message.",
        ]
        embed = discord.Embed(
            title="FishinTiffin Duck Game Help",
            description=(
                "Catch ducks, build your collection, and compete for the top.\n\n"
                "**How to Play**\n"
                "Use `!duck` in the ducks channel. Catches roll rarity and stats; some outcomes are much rarer than others.\n\n"
                "**Rarities**\n"
                "- Tiers run from **Common** up toward **Mythic** - the fancy stuff doesn't show up often.\n\n"
                "**Core Mechanics**\n"
                "- **Cooldown** shifts over time; it isn't a fixed timer.\n"
                "- **Stats** (attack/defense/speed) generally track how special the duck is.\n"
                "- **Shiny** is an extra-rare look, not a separate tier.\n"
                "- **Theft**: sometimes a catch comes from someone else's flock instead of the wild.\n"
                "- **Battle**: `!battle` matches your random eligible duck vs another player's; the winner keeps both flocks updated (one duck moves). Once per UTC hour unless you keep winning.\n"
                "- **Release**: `!release <name>` permanently removes your **newest** duck with that name (if you have several with the same name).\n"
                "- Stealing and payback can chain if timing lines up.\n"
                "- **Keish's ENERGY**: a rare streak where **Keish** comes to help you catch some ducks!\n"
                "- **Zay's ENERGY**: a rare face-off where **Zay** comes for your collection; **`!duck`** is how you respond.\n\n"
                "**Commands**\n"
                f"{chr(10).join(commands_lines)}"
            ),
            color=discord.Color.blurple(),
        )
        embed.set_footer(text="Tip: Use !ducks to track your full collection.")
        await ctx.reply(embed=embed, mention_author=True)

    @commands.command(name="duck")
    async def duck(self, ctx: commands.Context):
        if ctx.channel.id != self.bot.ducks:
            return await ctx.reply(
                "This command can only be used in the 🦆 ducks channel.", delete_after=5
            )
        try:
            result = await self._post_catch(
                str(ctx.author.id), ctx.guild.id if ctx.guild else None, ctx.channel.id
            )
            await self.dispatch_catch_result(ctx, result)
        except Exception as e:
            LOGGER.exception("duck command: %s", e)
            await ctx.reply("Something went wrong catching your duck. Please try again in a moment.")

    @commands.command(name="battle")
    async def battle(self, ctx: commands.Context):
        if ctx.channel.id != self.bot.ducks:
            return await ctx.reply(
                "This command can only be used in the 🦆 ducks channel.", delete_after=5
            )
        if ctx.guild is None:
            return await ctx.reply("Battles can only be used in a server.", mention_author=True)
        try:
            result = await self._post_battle(str(ctx.author.id), ctx.guild.id)
            await self.dispatch_catch_result(ctx, result)
        except Exception as e:
            LOGGER.exception("battle command: %s", e)
            await ctx.reply("Something went wrong with battle. Please try again in a moment.")

    @commands.command(name="ducks")
    async def ducks_list(self, ctx: commands.Context, member: discord.Member | None = None):
        target = member or ctx.author
        try:
            me = await self._get_me(str(target.id), ctx.guild.id if ctx.guild else None)
            count = me.get("duckCount")
            if count is None:
                count = len(me.get("ducks") or [])
            base = getattr(self.bot, "duck_dashboard_base_url", "https://api.duckgame.app/user").rstrip("/")
            url = f"{base}/{target.id}"
            if ctx.guild is not None:
                url = f"{url}?guild={ctx.guild.id}"
            await ctx.reply(
                f"{target.display_name} owns {count} duck{'s' if count != 1 else ''} 🦆\n"
                f"View them here: [Duck Dashboard]({url})"
            )
        except Exception as e:
            LOGGER.exception("ducks: %s", e)
            await ctx.reply("Could not load duck data from the API.")

    @commands.command(name="leaderboard")
    async def duck_leaderboard(self, ctx: commands.Context):
        try:
            season = getattr(self.bot, "leaderboard_season", "Preseason")
            data = await self._get_leaderboard(ctx.guild.id if ctx.guild else None)
            top = data.get("top") or []
            total_ducks = data.get("total_ducks", 0)
            if not top:
                await ctx.reply(f"No ducks have been caught yet! **Season:** {season}")
                return
            medals = {1: "🥇", 2: "🥈", 3: "🥉"}
            lines = []
            for i, row in enumerate(top, start=1):
                user_id = row["user_id"]
                count = row["count"]
                member = ctx.guild.get_member(int(user_id)) if ctx.guild else None
                name = member.display_name if member else f"User {user_id}"
                medal = medals.get(i, f"{i}.")
                lines.append(f"{medal} **{name}** - {count} duck{'s' if count != 1 else ''}")
            embed = discord.Embed(
                title=f"🏆 Duck Leaderboard - {season} 🏆",
                description="\n".join(lines),
                color=discord.Color.gold(),
            )
            embed.set_footer(text=f"Total ducks caught in the server: {total_ducks}")
            await ctx.reply(embed=embed)
        except Exception as e:
            LOGGER.exception("leaderboard: %s", e)
            await ctx.reply("Could not load leaderboard.")

    @commands.command(name="give")
    async def give_duck(self, ctx: commands.Context, member: discord.Member, *, duck_name: str):
        if ctx.channel.id != self.bot.ducks:
            return await ctx.reply(
                "This command can only be used in the 🦆 ducks channel.", delete_after=5
            )
        try:
            out = await self._post_give(
                str(ctx.author.id), str(member.id), duck_name.strip(),
                ctx.guild.id if ctx.guild else None,
            )
            await ctx.reply(
                f"🎁 {ctx.author.mention} gave **{out['duck_name']}** ({out['rarity']}) "
                f"to {member.mention}!"
            )
        except DuckApiError as e:
            await ctx.reply(str(e)[:2000])
        except Exception as e:
            LOGGER.exception("give failed: %s", e)
            await ctx.reply("Something went wrong with give. Please try again in a moment.")

    @commands.command(name="release")
    async def release_duck(self, ctx: commands.Context, *, duck_name: str):
        if ctx.channel.id != self.bot.ducks:
            return await ctx.reply(
                "This command can only be used in the 🦆 ducks channel.", delete_after=5
            )
        name = duck_name.strip()
        if not name:
            return await ctx.reply("Usage: `!release <duck_name>`", mention_author=True)
        try:
            out = await self._post_release(
                str(ctx.author.id),
                name,
                ctx.guild.id if ctx.guild else None,
            )
            await ctx.reply(
                f"🌊 {ctx.author.mention} released **{out['duck_name']}** ({out['rarity']}) back into the wild."
            )
        except DuckApiError as e:
            await ctx.reply(str(e)[:2000])
        except Exception as e:
            LOGGER.exception("release failed: %s", e)
            await ctx.reply("Something went wrong with release. Please try again in a moment.")

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        await self._handle_duck_role_reaction(payload, added=True)

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent):
        await self._handle_duck_role_reaction(payload, added=False)

    async def _handle_duck_role_reaction(
        self, payload: discord.RawReactionActionEvent, *, added: bool
    ) -> None:
        if payload.guild_id is None or payload.user_id == self.bot.user.id:
            return
        if str(payload.emoji) != DUCK_ROLE_EMOJI:
            return

        roles_channel = getattr(self.bot, "roles_channel", None)
        if roles_channel is not None and payload.channel_id != roles_channel:
            return

        role_id = getattr(self.bot, "duck_role", None)
        if role_id is None:
            return

        channel = self.bot.get_channel(payload.channel_id)
        if channel is None:
            return
        try:
            message = await channel.fetch_message(payload.message_id)
        except (discord.Forbidden, discord.NotFound, discord.HTTPException):
            return
        if message.author.id != self.bot.user.id:
            return

        guild = self.bot.get_guild(payload.guild_id)
        if guild is None:
            return
        role = guild.get_role(role_id)
        if role is None:
            return

        try:
            member = guild.get_member(payload.user_id) or await guild.fetch_member(payload.user_id)
        except discord.HTTPException:
            return
        if member.bot:
            return

        try:
            if added and role not in member.roles:
                await member.add_roles(role, reason="Reacted to duck onboarding role message")
            elif not added and role in member.roles:
                await member.remove_roles(role, reason="Removed reaction from duck onboarding role message")
        except discord.Forbidden:
            LOGGER.info("[duck-role] Missing permission to %s role", "add" if added else "remove")
        except discord.HTTPException:
            LOGGER.info("[duck-role] Failed to change role for user %s", payload.user_id)

    # ---- Announcement relay ------------------------------------------
    # When duck_game_api_shared_secret is set, the bot polls the backend for catch embeds
    # that originated from the web UI and relays them into Discord.

    async def cog_load(self) -> None:
        if not getattr(self.bot, "duck_game_api_shared_secret", None):
            LOGGER.info("[announce-poll] disabled (no duck_game_api_shared_secret configured)")
            return
        if not self.poll_announcements.is_running():
            self.poll_announcements.start()
            LOGGER.info("[announce-poll] started")

    async def cog_unload(self) -> None:
        if self.poll_announcements.is_running():
            self.poll_announcements.cancel()

    def _bot_headers(self) -> dict[str, str] | None:
        secret = getattr(self.bot, "duck_game_api_shared_secret", None)
        return {"x-bot-token": str(secret)} if secret else None

    async def _fetch_pending_announcements(self) -> list[dict]:
        headers = self._bot_headers()
        session: aiohttp.ClientSession | None = self.bot.http_session
        if headers is None or session is None:
            return []
        url = f"{self.api_base}/v1/bot/pending-announcements?limit=20"
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    LOGGER.warning("[announce-poll] GET %s -> %s: %s", url, resp.status, body[:200])
                    return []
                data = await resp.json()
        except (aiohttp.ClientError, TimeoutError) as e:
            LOGGER.debug("[announce-poll] fetch failed: %s", e)
            return []
        items = data.get("items") if isinstance(data, dict) else None
        return items if isinstance(items, list) else []

    async def _ack_announcements(self, ids: list[int]) -> None:
        if not ids:
            return
        headers = self._bot_headers()
        session: aiohttp.ClientSession | None = self.bot.http_session
        if headers is None or session is None:
            return
        url = f"{self.api_base}/v1/bot/announcements/ack"
        try:
            async with session.post(
                url, headers=headers, json={"ids": ids}, timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    LOGGER.warning("[announce-poll] ack %s -> %s: %s", ids, resp.status, body[:200])
        except (aiohttp.ClientError, TimeoutError) as e:
            LOGGER.debug("[announce-poll] ack failed: %s", e)

    async def _resolve_author(
        self, guild: discord.Guild | None, user_id: int
    ) -> discord.abc.User | discord.Member | None:
        if guild is not None:
            member = guild.get_member(user_id)
            if member is not None:
                return member
            try:
                return await guild.fetch_member(user_id)
            except discord.HTTPException:
                pass
        user = self.bot.get_user(user_id)
        if user is not None:
            return user
        try:
            return await self.bot.fetch_user(user_id)
        except discord.HTTPException:
            return None

    @tasks.loop(seconds=2.0)
    async def poll_announcements(self) -> None:
        items = await self._fetch_pending_announcements()
        if not items:
            return
        delivered: list[int] = []
        for row in items:
            try:
                row_id = int(row["id"])
                guild_id = int(row["guild_id"])
                channel_id = int(row["channel_id"])
                user_id = int(row["user_id"])
                payload = row.get("payload") or {}
            except (KeyError, TypeError, ValueError):
                LOGGER.warning("[announce-poll] malformed row: %r", row)
                continue

            channel = self.bot.get_channel(channel_id)
            if channel is None:
                try:
                    channel = await self.bot.fetch_channel(channel_id)
                except (discord.NotFound, discord.Forbidden, discord.HTTPException) as e:
                    LOGGER.warning("[announce-poll] cannot resolve channel %s: %s", channel_id, e)
                    continue
            if channel is None or not isinstance(channel, discord.abc.Messageable):
                LOGGER.debug("[announce-poll] channel %s not messageable", channel_id)
                continue
            guild = self.bot.get_guild(guild_id)
            author = await self._resolve_author(guild, user_id)
            synth = _SyntheticCtx(self.bot, channel, guild, author, user_id)
            try:
                await self.dispatch_catch_result(synth, payload)
            except Exception:
                # Still ack - retrying a broken payload forever would wedge the queue.
                LOGGER.exception("[announce-poll] dispatch failed for row %s", row_id)
            delivered.append(row_id)
        await self._ack_announcements(delivered)

    @poll_announcements.before_loop
    async def _before_poll(self) -> None:
        await self.bot.wait_until_ready()
