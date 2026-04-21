# duck_manager.py — Duck game cog (!duck, DB, Keish/Zay energy).
from discord.ext import commands
from datetime import datetime, timedelta, timezone
import discord
import aiohttp
import asyncio
import sqlite3
import json
import random
import uuid
from pathlib import Path
import logging
from dataclasses import dataclass, field

from .duck_dashboard_html import generate_duck_dashboard_html
from .duck_clock import utc_ts
from .keish_energy import (
    KEISH_ENERGY_GIF_PATH,
    KEISH_FLOCK_CATCH_TITLE,
    KEISH_FLOCK_MAX,
    KEISH_FLOCK_MIN,
    KEISH_FLOCK_ROLLS,
    KEISH_PROC_TITLE,
    KeishEnergy,
    keish_proc_description,
    pick_keish_success_image_path,
)
from .items import ARI_PIE_COOLDOWN_DIVISOR, BIG_GUY_STAT_BONUS, ItemsManager
from .paths import ASSETS_DIR, DUCK_DATA_DIR, HTML_DIR
from .weather import WEATHER_EMBED_COLOR, WeatherManager, weather_catch_label
from .zay_energy import (
    ZAY_DEFENSE_PROBS,
    ZAY_ENERGY_GIF_PATH,
    ZAY_PROC_TITLE,
    ZayEnergy,
    zay_proc_description,
    zay_proc_footer,
)

LOGGER = logging.getLogger("fishin_tiffin.ducks")
DUCK_ROLE_EMOJI = "🦆"

# Commands allowed outside `ducks_channel` (reference / read-only).
DUCK_INFO_COMMAND_NAMES = frozenset(
    {"help", "items", "weathers", "energies", "ducks", "leaderboard"}
)


def _log(message: str):
    LOGGER.info(message)


DUCK_DATA_DIR.mkdir(parents=True, exist_ok=True)
HTML_DIR.mkdir(parents=True, exist_ok=True)

NAMES_COMMON_PATH = DUCK_DATA_DIR / "names_common.json"      # used by Common/Uncommon/Rare
NAMES_LEGENDARY_PATH = DUCK_DATA_DIR / "names_legendary.json"  # used by Legendary/Mythic

DEFAULT_NAMES_COMMON = []
DEFAULT_NAMES_LEGENDARY = []

RARITY_WEIGHTS = [
    ("Common", 65),
    ("Uncommon", 20),
    ("Rare", 10),
    ("Legendary", 4),
    ("Mythic", 1),
]

RARITY_EMBED_COLORS = {
    "Common": discord.Color.light_grey(),
    "Uncommon": discord.Color.green(),
    "Rare": discord.Color.blue(),
    "Legendary": discord.Color.orange(),
    "Mythic": discord.Color.purple(),
}
RARITY_CATCH_FLAIR = {
    "Common": "A scrappy little duck waddles your way.",
    "Uncommon": "A curious duck tilts its head and joins you.",
    "Rare": "A striking duck circles once, then lands at your side.",
    "Legendary": "The skies part as a mighty duck descends!",
    "Mythic": "Reality shimmers… a mythical duck chooses you.",
}

# Shiny cosmetic chance (%)
SHINY_PROB = 1.0
BOOT_IMAGE_URLS = [
    "https://preview.redd.it/weinbrenner-boots-or-boot-singular-from-the-50s-or-60s-v0-yvf5l7k7pc9a1.jpg?width=1080&crop=smart&auto=webp&s=bde8de1772b2ee55dce93d784148a4f6dbebaf20",
    "https://images.nms.ac.uk/production/Images/Discover/Boots-not-made-for-walking-a-rare-pair-of-Jack-boots/2025-07-14-Jack-boot.jpg?w=1200&h=900&q=100&auto=format&fit=crop&crop=focalpoint&fp-x=0.5016&fp-y=0.6521&dm=1754643787&s=c5260198a2704eedb5c2b5965e4be19b",
    "https://preview.free3d.com/img/2019/07/2273113531614233936/tsko21nn.jpg",
    "https://cdn.pixabay.com/photo/2015/10/29/21/28/boot-1013034_1280.jpg",
    "https://seaofthieves.wiki.gg/images/Old_Boot.png",
]

# Primary `!duck` outcome weights (percent-like relative weights).
# Keep these explicit so event chances are transparent and easy to extend.
DUCK_OUTCOME_WEIGHTS = [
    ("zay_proc", 1),
    ("keish_proc", 1),
    ("weather_proc", 2),
    ("item_proc", 2),
    ("boot", 3),
    ("steal", 15),
    ("new_duck", 76),
]

# Cooldown parameters
COOLDOWN_MEAN = 30.0  # seconds
COOLDOWN_STD = 120.0   # seconds
COOLDOWN_MIN = 1      # seconds
COOLDOWN_MAX = 5 * 60  # 5 minutes

REVENGE_WINDOW_SECONDS = 5 * 60
REVENGE_SWING_STEAL_THRESHOLD = 20

BATTLE_CHALLENGE_TIMEOUT_SECONDS = 90

# Stat weighting (1..10) — rarity shifts probability mass higher
STAT_WEIGHTS = {
    "Common":    [33, 22, 17, 11, 9, 8, 0, 0, 0, 0],   # total 100
    "Uncommon":  [13, 15, 15, 15, 15, 15, 12, 0, 0, 0],# total 100
    "Rare":      [4, 5, 8, 11, 14, 16, 20, 22, 0, 0],  # total 100
    "Legendary": [1, 1, 2, 3, 5, 8, 12, 18, 24, 26],   # total 100 (unchanged)
    "Mythic":    [0, 0, 0, 0, 2, 5, 8, 15, 30, 40],    # total 100 (unchanged)
}

def _ensure_json_file(path: Path, default_payload: list):
    if not path.exists():
        path.write_text(json.dumps(default_payload, indent=2))

def _load_names(path: Path) -> list:
    """
    Loads names from JSON file (simple array format).
    Automatically migrates old format (dict with available/used) if detected.
    """
    _ensure_json_file(path, DEFAULT_NAMES_COMMON if "common" in path.name else DEFAULT_NAMES_LEGENDARY)
    with path.open("r") as f:
        payload = json.load(f)
    
    # Handle old format migration (for backward compatibility)
    if isinstance(payload, dict):
        names = payload.get("available", []) + payload.get("used", [])
        # Remove duplicates while preserving order
        names_unique = []
        seen = set()
        for name in names:
            if name not in seen:
                seen.add(name)
                names_unique.append(name)
        # Save migrated format
        with path.open("w") as f:
            json.dump(names_unique, f, indent=2)
        return names_unique
    
    # New format: already a list
    return payload if isinstance(payload, list) else []

def _pick_name_for_rarity(rarity: str) -> str:
    """
    Picks a random name from the appropriate pool. Names can be reused.
    Raises RuntimeError if no names are available.
    """
    path = NAMES_COMMON_PATH if rarity in ("Common","Uncommon","Rare") else NAMES_LEGENDARY_PATH
    names = _load_names(path)
    if not names:
        raise RuntimeError(f"No names available in {path.name}. Add more names to continue.")
    
    return random.choice(names)

def _roll_rarity() -> str:
    labels, weights = zip(*RARITY_WEIGHTS)
    return random.choices(labels, weights=weights, k=1)[0]

def _roll_stat(rarity: str) -> int:
    # Roll integer 1..10 using rarity-specific weights
    weights = STAT_WEIGHTS[rarity]
    vals = list(range(1, 11))
    return random.choices(vals, weights=weights, k=1)[0]

def _roll_shiny(mult: float = 1.0) -> bool:
    return random.uniform(0, 100) < SHINY_PROB * mult


def _roll_duck_outcome(
    *,
    allow_zay_proc: bool,
    allow_keish_proc: bool,
    allow_weather_proc: bool,
    allow_item_proc: bool,
    allow_boot: bool,
) -> str:
    labels = []
    weights = []
    for label, weight in DUCK_OUTCOME_WEIGHTS:
        if label == "zay_proc" and not allow_zay_proc:
            continue
        if label == "keish_proc" and not allow_keish_proc:
            continue
        if label == "weather_proc" and not allow_weather_proc:
            continue
        if label == "item_proc" and not allow_item_proc:
            continue
        if label == "boot" and not allow_boot:
            continue
        labels.append(label)
        weights.append(weight)
    return random.choices(labels, weights=weights, k=1)[0]


def _pick_boot_image_url() -> str:
    if not BOOT_IMAGE_URLS:
        # Safe fallback so an empty list does not break embeds.
        return "https://seaofthieves.wiki.gg/images/Old_Boot.png"
    return random.choice(BOOT_IMAGE_URLS)

def _roll_cooldown_seconds() -> int:
    # Normal distribution, clamped to [COOLDOWN_MIN, COOLDOWN_MAX]
    val = random.gauss(COOLDOWN_MEAN, COOLDOWN_STD)
    val = max(COOLDOWN_MIN, min(COOLDOWN_MAX, val))
    return int(round(val))


def _fmt_duration(seconds: int) -> str:
    m, s = divmod(seconds, 60)
    if m <= 0:
        return f"{s}s"
    if s == 0:
        return f"{m}m"
    return f"{m}m {s}s"


def _generate_duck_id() -> str:
    # Generate a unique ID for each duck catch
    return str(uuid.uuid4())


def build_duck_onboarding_embed(role_mention: str) -> discord.Embed:
    embed = discord.Embed(
        title="Join the Duck Game",
        description=(
            f"React with {DUCK_ROLE_EMOJI} below. "
            f"React to add {role_mention} or react then remove reaction to remove the role.\n\n"
            "**How the game works**\n"
            "- Use `!duck` in the ducks channel to catch ducks.\n"
            "- Catches roll rarity and stats; some ducks are extra special.\n"
            "- `!duck` has a randomized cooldown per successful catch.\n"
            "- Sometimes your `!duck` steals one from another player.\n"
            "- If **Zay** shows up, **`!duck`** up to **three times** to defend your flock (he leaves after a failed block).\n\n"
            "- If **Keish** shows up, **he** lines up **three** special **`!duck`** pulls—each one nets a **flock** of new ducks (**no cooldown** between them).\n\n"
            "**Rarities**\n"
            "- From **Common** up to **Mythic**—the higher tiers show up less often.\n\n"
            "Use `!help` anytime for commands and how things work."
        ),
        color=discord.Color.blurple(),
    )
    return embed


DUCK_TYPO_GIF_PATH = ASSETS_DIR / "dap.gif"


def _levenshtein(a: str, b: str) -> int:
    """Classic Levenshtein distance (insert/delete/substitute)."""
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
            ins = cur[j - 1] + 1
            delete = prev[j] + 1
            sub = prev[j - 1] + (ca != cb)
            cur.append(min(ins, delete, sub))
        prev = cur
    return prev[-1]


def _first_bang_command_token(content: str) -> str | None:
    text = content.strip()
    if not text.startswith("!"):
        return None
    rest = text[1:].strip()
    if not rest:
        return None
    return rest.split()[0]


def _token_looks_like_duck_typo(token: str) -> bool:
    """True when the token is one cheap edit away from 'duck' (covers fuck/dick/suck, dock, duk, etc.)."""
    t = token.lower()
    if len(t) < 3 or len(t) > 7:
        return False
    return _levenshtein(t, "duck") <= 1


async def try_consume_duck_typo(bot: commands.Bot, message: discord.Message) -> bool:
    """
    If the message looks like a mistyped !duck in the ducks channel, handle it and return True
    so the bot skips normal command processing.
    """
    if message.author.bot:
        return False
    ducks_ch = getattr(bot, "ducks", None)
    if ducks_ch is None or message.channel.id != ducks_ch:
        return False
    token = _first_bang_command_token(message.content)
    if not token:
        return False
    low = token.lower()
    if low == "duck":
        return False
    if low == "ducks":
        return False
    if not _token_looks_like_duck_typo(token):
        return False
    cog = bot.get_cog("DuckManager")
    if cog is None:
        return False
    await cog.handle_duck_typo(message)
    return True


@dataclass
class _PendingBattleChallenge:
    """In-flight `!battle @user` until accept, deny, or timeout."""

    guild_id: int
    challenger_id: str
    defender_id: str
    channel_id: int
    timeout_task: asyncio.Task | None = field(default=None, repr=False)


class DuckManager(commands.Cog, name="DuckManager"):
    """
    Duck game Cog:
      - !duck : catch a duck via Duck API
      - (hooks) !ducks, !duck-leaderboard
    """

    def __init__(self, bot, duck_api_url: str = "https://duck.jocal.dev/duck"):
        self.bot = bot
        self.duck_api_url = duck_api_url.rstrip("/")
        self.dashboard_base_url = getattr(bot, "duck_dashboard_base_url", "https://duck.jocal.dev/user").rstrip("/")
        self.db = sqlite3.connect(str(DUCK_DATA_DIR / "ducks.db"))
        self.db.row_factory = sqlite3.Row
        self.keish = KeishEnergy()
        self.zay = ZayEnergy(bot, self)
        self.weather = WeatherManager()
        self.items = ItemsManager()
        # Targeted `!battle @member`: at most one pending incoming per defender and one outgoing per challenger per guild.
        self._pending_battle_by_defender: dict[tuple[int, str], _PendingBattleChallenge] = {}
        self._pending_battle_by_challenger: dict[tuple[int, str], _PendingBattleChallenge] = {}
        self._init_db()
        _log("[DuckManager] initialized")

    async def cog_check(self, ctx: commands.Context) -> bool:
        configured_server = getattr(self.bot, "server", None)
        if configured_server is not None and ctx.guild is not None:
            if ctx.guild.id != configured_server:
                return False

        if ctx.command is None:
            return True

        if ctx.command.name.lower() in DUCK_INFO_COMMAND_NAMES:
            return True

        ducks_ch = getattr(self.bot, "ducks", None)
        if ducks_ch is None:
            return True

        if ctx.guild is None:
            await ctx.reply(
                "Duck game commands only work in a server, in the 🦆 ducks channel.",
                delete_after=10,
                mention_author=False,
            )
            return False

        if ctx.channel.id != ducks_ch:
            await ctx.reply(
                "Duck game commands only work in the 🦆 ducks channel. Use `!help` anywhere for rules.",
                delete_after=8,
                mention_author=False,
            )
            return False

        return True

    def cog_unload(self):
        self.zay.cancel_all_tasks()
        for pending in list(self._pending_battle_by_defender.values()):
            if pending.timeout_task and not pending.timeout_task.done():
                pending.timeout_task.cancel()
        self._pending_battle_by_defender.clear()
        self._pending_battle_by_challenger.clear()
        self.db.close()

    async def handle_duck_typo(self, message: discord.Message) -> None:
        ctx = await self.bot.get_context(message)
        if not await self.cog_check(ctx):
            return
        typo_gif_file = discord.File(DUCK_TYPO_GIF_PATH, filename="dap.gif")
        await message.reply(
            f"{message.author.mention} dont worry fam i got you",
            file=typo_gif_file,
            mention_author=False,
        )
        try:
            await self.duck(ctx)
        finally:
            try:
                await message.delete()
            except discord.HTTPException:
                pass

    # ---------- DB SETUP ----------
    def _init_db(self):
        cur = self.db.cursor()
        
        # Check if ducks table exists and has UNIQUE constraint on name
        cur.execute("""
            SELECT sql FROM sqlite_master 
            WHERE type='table' AND name='ducks'
        """)
        table_sql = cur.fetchone()
        
        # If table exists with UNIQUE constraint, migrate it
        if table_sql and 'UNIQUE' in table_sql[0] and 'name' in table_sql[0]:
            _log("[DuckManager] Migrating ducks table to remove UNIQUE constraint on name")
            # Create new table without UNIQUE constraint
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ducks_new (
                    duck_id   TEXT PRIMARY KEY,
                    url       TEXT NOT NULL,
                    rarity    TEXT NOT NULL,
                    name      TEXT NOT NULL,
                    attack    INTEGER NOT NULL,
                    defense   INTEGER NOT NULL,
                    speed     INTEGER NOT NULL,
                    shiny     INTEGER NOT NULL DEFAULT 0,
                    timestamp INTEGER NOT NULL,
                    owner_id  TEXT
                )
            """)
            # Copy data
            cur.execute("""
                INSERT INTO ducks_new 
                SELECT duck_id, url, rarity, name, attack, defense, speed, shiny, timestamp, owner_id
                FROM ducks
            """)
            # Drop old table
            cur.execute("DROP TABLE ducks")
            # Rename new table
            cur.execute("ALTER TABLE ducks_new RENAME TO ducks")
            self.db.commit()
            _log("[DuckManager] Migration complete")
        
        # Create table if it doesn't exist (or was just migrated/dropped)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ducks (
                duck_id   TEXT PRIMARY KEY,   -- unique ID (UUID) for each duck catch
                url       TEXT NOT NULL,
                rarity    TEXT NOT NULL,
                name      TEXT NOT NULL,      -- names can be reused
                attack    INTEGER NOT NULL,
                defense   INTEGER NOT NULL,
                speed     INTEGER NOT NULL,
                shiny     INTEGER NOT NULL DEFAULT 0,
                timestamp INTEGER NOT NULL,   -- UNIX when API was called
                owner_id  TEXT                -- discord user id (current owner)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id    TEXT PRIMARY KEY,  -- discord user id
                ducks_json TEXT NOT NULL,     -- JSON array of duck_ids they own (strings)
                last_catch INTEGER,           -- UNIX timestamp of last successful catch
                cooldown   INTEGER            -- seconds assigned on last catch
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pending_revenge (
                victim_id      TEXT NOT NULL,    -- user who can trigger revenge
                thief_id       TEXT NOT NULL,    -- current/initial thief
                stolen_duck_id TEXT NOT NULL,    -- duck that was stolen
                created_ts     INTEGER NOT NULL,
                expires_ts     INTEGER NOT NULL
            )
        """)

        # Idempotent !battle column migration (one battle per UTC hour bucket + win streak bypass).
        cur.execute("PRAGMA table_info(users)")
        existing_cols = {row["name"] for row in cur.fetchall()}
        if "battle_hour_bucket" not in existing_cols:
            cur.execute("ALTER TABLE users ADD COLUMN battle_hour_bucket INTEGER")
        if "battle_streak" not in existing_cols:
            cur.execute("ALTER TABLE users ADD COLUMN battle_streak INTEGER")
        self.db.commit()

    # ---------- HELPERS ----------
    def _get_user(self, user_id: str) -> sqlite3.Row | None:
        cur = self.db.cursor()
        cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        return cur.fetchone()

    def _duck_catch_weather_effects_lines(self, user_id: str) -> str:
        """Italic weather + item-effect summary for catch embeds (above **Name:**)."""
        w = weather_catch_label(self.weather.current)
        item_labels = self.items.active_item_effect_labels(user_id)
        effects = "None" if not item_labels else ", ".join(item_labels)
        return f"*Weather: {w}*\n*Effects: {effects}*\n\n"

    def _pvp_weather_effects_lines(
        self, user_a_id: str, user_b_id: str, *, label_a: str, label_b: str
    ) -> str:
        """Italic weather + per-fighter item effects (same style as duck catch embed)."""
        w = weather_catch_label(self.weather.current)
        la = self.items.active_item_effect_labels(user_a_id)
        lb = self.items.active_item_effect_labels(user_b_id)
        eff_a = "None" if not la else ", ".join(la)
        eff_b = "None" if not lb else ", ".join(lb)
        return (
            f"*Weather: {w}*\n"
            f"*{label_a}: {eff_a}*\n"
            f"*{label_b}: {eff_b}*\n\n"
        )

    def _set_user(self, user_id: str, ducks_list: list[str], last_catch: int | None, cooldown: int | None):
        cur = self.db.cursor()
        cur.execute("""
            INSERT INTO users (user_id, ducks_json, last_catch, cooldown)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                ducks_json=excluded.ducks_json,
                last_catch=excluded.last_catch,
                cooldown=excluded.cooldown
        """, (user_id, json.dumps(ducks_list), last_catch, cooldown))
        self.db.commit()

    def _add_duck_to_user(self, user_id: str, duck_id: str):
        row = self._get_user(user_id)
        ducks_list = []
        if row:
            ducks_list = json.loads(row["ducks_json"]) if row["ducks_json"] else []
        if duck_id not in ducks_list:
            ducks_list.append(duck_id)
        self._set_user(user_id, ducks_list, row["last_catch"] if row else None, row["cooldown"] if row else None)

    def _remove_duck_from_user(self, user_id: str, duck_id: str):
        row = self._get_user(user_id)
        if not row:
            return
        ducks_list = json.loads(row["ducks_json"]) if row["ducks_json"] else []
        if duck_id in ducks_list:
            ducks_list.remove(duck_id)
        self._set_user(user_id, ducks_list, row["last_catch"], row["cooldown"])

    def _get_owned_duck_row_by_name(self, user_id: str, duck_name: str) -> sqlite3.Row | None:
        """Duck row for this name if it appears in the user's collection (handles global duplicate names)."""
        urow = self._get_user(user_id)
        if not urow or not urow["ducks_json"]:
            return None
        duck_ids = json.loads(urow["ducks_json"])
        if not duck_ids:
            return None
        cur = self.db.cursor()
        q_marks = ",".join("?" * len(duck_ids))
        cur.execute(
            f"SELECT * FROM ducks WHERE duck_id IN ({q_marks}) AND name = ? LIMIT 1",
            (*duck_ids, duck_name),
        )
        return cur.fetchone()

    async def _fetch_duck_url(self) -> tuple[str, int]:
        """
        Calls duck API (JSON with {'url': '...'}) and returns (url, timestamp).
        """
        ts = int(datetime.now(timezone.utc).timestamp())
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.duck_api_url}") as resp:
                resp.raise_for_status()
                payload = await resp.json()
                url = payload.get("url")
                if not url:
                    raise RuntimeError("Duck API returned no 'url' field.")
                return url, ts

    def _create_duck_record(self, duck_id: str, url: str, timestamp: int) -> sqlite3.Row:
        """
        Creates a new duck record in DB with assigned rarity/name/stats/shiny.
        Returns the duck row.
        """
        rarity = _roll_rarity()
        attack = _roll_stat(rarity)
        defense = _roll_stat(rarity)
        speed = _roll_stat(rarity)
        shiny = 1 if _roll_shiny(self.weather.shiny_multiplier()) else 0
        name = _pick_name_for_rarity(rarity)
        # If shiny, add sparkle to name
        if shiny:
            name = f"✨{name}✨"

        cur = self.db.cursor()
        cur.execute("""
            INSERT INTO ducks (duck_id, url, rarity, name, attack, defense, speed, shiny, timestamp, owner_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        """, (duck_id, url, rarity, name, attack, defense, speed, shiny, timestamp))
        self.db.commit()

        cur.execute("SELECT * FROM ducks WHERE duck_id = ?", (duck_id,))
        return cur.fetchone()

    def _set_duck_owner(self, duck_id: str, owner_id: str):
        cur = self.db.cursor()
        cur.execute("""
            UPDATE ducks SET owner_id = ?
            WHERE duck_id = ?
        """, (owner_id, duck_id))
        self.db.commit()

    def _update_cooldown(self, user_id: str) -> int:
        cd = _roll_cooldown_seconds()
        cd = max(1, round(cd * self.weather.cooldown_multiplier() / self.items.cooldown_divisor(user_id)))
        now_ts = int(datetime.now(timezone.utc).timestamp())
        row = self._get_user(user_id)
        ducks_list = json.loads(row["ducks_json"]) if row and row["ducks_json"] else []
        self._set_user(user_id, ducks_list, now_ts, cd)
        return cd

    def _refresh_user_cooldown(self, user_id: str):
        """Reset user cooldown so they can use !duck immediately."""
        row = self._get_user(user_id)
        ducks_list = json.loads(row["ducks_json"]) if row and row["ducks_json"] else []
        self._set_user(user_id, ducks_list, None, None)

    def _obliterate_duck(self, user_id: str, duck_id: str):
        self._remove_duck_from_user(user_id, duck_id)
        cur = self.db.cursor()
        cur.execute("DELETE FROM ducks WHERE duck_id = ?", (duck_id,))
        self.db.commit()

    def _check_on_cooldown(self, user_id: str) -> tuple[bool, int]:
        if self.keish.active(user_id):
            return False, 0
        if self.zay.active(user_id):
            return False, 0
        row = self._get_user(user_id)
        if not row or not row["last_catch"] or not row["cooldown"]:
            return False, 0
        now_ts = utc_ts()
        ready_ts = row["last_catch"] + row["cooldown"]
        if now_ts < ready_ts:
            return True, ready_ts - now_ts
        return False, 0

    def _is_stealable_duck(self, duck_row: sqlite3.Row) -> bool:
        """Legendary/Mythic/Shiny ducks are protected from all steals."""
        if duck_row is None:
            return False
        if bool(duck_row["shiny"]):
            return False
        return duck_row["rarity"] not in ("Legendary", "Mythic")

    @staticmethod
    def _battle_hour_bucket_now() -> int:
        return utc_ts() // 3600

    @staticmethod
    def _seconds_until_next_utc_hour() -> int:
        now = datetime.now(timezone.utc)
        next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        return max(1, int((next_hour - now).total_seconds()))

    def _can_battle(self, user_id: str) -> tuple[bool, int | None]:
        """Whether the user may start or continue a !battle chain (UTC hour bucket + streak)."""
        bucket = self._battle_hour_bucket_now()
        row = self._get_user(user_id)
        if row is None:
            return True, None
        row_bucket = row["battle_hour_bucket"]
        if row_bucket is None or row_bucket != bucket:
            return True, None
        if row["battle_streak"]:
            return True, None
        return False, self._seconds_until_next_utc_hour()

    def _set_battle_state(self, user_id: str, bucket: int, streak: bool) -> None:
        row = self._get_user(user_id)
        if row is None:
            return
        cur = self.db.cursor()
        cur.execute(
            "UPDATE users SET battle_hour_bucket = ?, battle_streak = ? WHERE user_id = ?",
            (bucket, 1 if streak else 0, user_id),
        )
        self.db.commit()

    def _get_user_duck_ids(self, user_id: str) -> list[str]:
        row = self._get_user(user_id)
        if not row or not row["ducks_json"]:
            return []
        return json.loads(row["ducks_json"])

    def _get_duck(self, duck_id: str) -> sqlite3.Row | None:
        cur = self.db.cursor()
        cur.execute("SELECT * FROM ducks WHERE duck_id = ?", (duck_id,))
        return cur.fetchone()

    def _get_random_stealable_duck_from_user(
        self,
        user_id: str,
        exclude_duck_ids: set[str] | None = None,
    ) -> str | None:
        exclude_duck_ids = exclude_duck_ids or set()
        eligible_duck_ids = []
        for duck_id in self._get_user_duck_ids(user_id):
            if duck_id in exclude_duck_ids:
                continue
            duck_row = self._get_duck(duck_id)
            if duck_row and self._is_stealable_duck(duck_row):
                eligible_duck_ids.append(duck_id)
        if not eligible_duck_ids:
            return None
        return random.choice(eligible_duck_ids)

    def _get_random_duck_from_user(
        self,
        user_id: str,
        exclude_duck_ids: set[str] | None = None,
    ) -> str | None:
        """Any owned duck (including Legendary/Mythic/Shiny) — for battle fighters only."""
        exclude_duck_ids = exclude_duck_ids or set()
        eligible_duck_ids = []
        for duck_id in self._get_user_duck_ids(user_id):
            if duck_id in exclude_duck_ids:
                continue
            duck_row = self._get_duck(duck_id)
            if duck_row:
                eligible_duck_ids.append(duck_id)
        if not eligible_duck_ids:
            return None
        return random.choice(eligible_duck_ids)

    def _get_random_user_with_stealable_ducks(
        self,
        exclude_user_id: str = None,
        allowed_user_ids: set[str] | None = None,
    ) -> tuple[str, str] | None:
        """
        Returns a random (user_id, duck_id) tuple from a user who has stealable ducks.
        exclude_user_id: user to exclude from selection
        allowed_user_ids: if provided, only users in this set are eligible (e.g. current guild members).
        Returns None if no users with stealable ducks exist (other than excluded user).
        """
        cur = self.db.cursor()
        cur.execute("SELECT user_id, ducks_json FROM users WHERE ducks_json IS NOT NULL AND ducks_json != '[]' AND ducks_json != ''")
        rows = cur.fetchall()

        candidates = []
        for row in rows:
            user_id = row["user_id"]
            if exclude_user_id and user_id == exclude_user_id:
                continue
            if allowed_user_ids is not None and user_id not in allowed_user_ids:
                continue
            duck_ids = json.loads(row["ducks_json"]) if row["ducks_json"] else []
            eligible_duck_ids = []
            for duck_id in duck_ids:
                duck_row = self._get_duck(duck_id)
                if duck_row and self._is_stealable_duck(duck_row):
                    eligible_duck_ids.append(duck_id)
            if eligible_duck_ids:
                candidates.append((user_id, eligible_duck_ids))

        if not candidates:
            return None

        random_user_id, duck_ids = random.choice(candidates)
        random_duck_id = random.choice(duck_ids)
        return (random_user_id, random_duck_id)

    def _get_random_user_with_ducks(
        self,
        exclude_user_id: str = None,
        allowed_user_ids: set[str] | None = None,
    ) -> tuple[str, str] | None:
        """
        Returns a random (user_id, duck_id) from any user who owns at least one duck.
        Same filtering as _get_random_user_with_stealable_ducks but any duck qualifies.
        """
        cur = self.db.cursor()
        cur.execute("SELECT user_id, ducks_json FROM users WHERE ducks_json IS NOT NULL AND ducks_json != '[]' AND ducks_json != ''")
        rows = cur.fetchall()

        candidates = []
        for row in rows:
            user_id = row["user_id"]
            if exclude_user_id and user_id == exclude_user_id:
                continue
            if allowed_user_ids is not None and user_id not in allowed_user_ids:
                continue
            duck_ids = json.loads(row["ducks_json"]) if row["ducks_json"] else []
            eligible_duck_ids = []
            for duck_id in duck_ids:
                duck_row = self._get_duck(duck_id)
                if duck_row:
                    eligible_duck_ids.append(duck_id)
            if eligible_duck_ids:
                candidates.append((user_id, eligible_duck_ids))

        if not candidates:
            return None

        random_user_id, duck_ids = random.choice(candidates)
        random_duck_id = random.choice(duck_ids)
        return (random_user_id, random_duck_id)

    async def _guild_member_ids(self, guild: discord.Guild | None) -> set[str] | None:
        """Return ``{str(member.id) for member in guild.members}`` or ``None`` if unavailable.

        Chunks the guild once so ``guild.members`` is populated on gateway reconnects.
        Used to restrict steal/battle targets and leaderboard ranks to people currently in the server.
        """
        if guild is None:
            return None
        if not guild.chunked:
            try:
                await guild.chunk()
            except discord.HTTPException as exc:
                LOGGER.warning("guild member chunk failed (steal filter skipped): %s", exc)
                return None
        return {str(m.id) for m in guild.members}

    def _set_pending_revenge(self, victim_id: str, thief_id: str, stolen_duck_id: str):
        now_ts = int(datetime.now(timezone.utc).timestamp())
        expires_ts = now_ts + REVENGE_WINDOW_SECONDS
        cur = self.db.cursor()
        cur.execute(
            """
            INSERT INTO pending_revenge (victim_id, thief_id, stolen_duck_id, created_ts, expires_ts)
            VALUES (?, ?, ?, ?, ?)
            """,
            (victim_id, thief_id, stolen_duck_id, now_ts, expires_ts),
        )
        self.db.commit()

    def _get_pending_revenge(self, victim_id: str) -> sqlite3.Row | None:
        cur = self.db.cursor()
        now_ts = int(datetime.now(timezone.utc).timestamp())
        cur.execute(
            "DELETE FROM pending_revenge WHERE victim_id = ? AND expires_ts < ?",
            (victim_id, now_ts),
        )
        cur.execute(
            """
            SELECT * FROM pending_revenge
            WHERE victim_id = ?
            ORDER BY created_ts ASC
            LIMIT 1
            """,
            (victim_id,),
        )
        row = cur.fetchone()
        self.db.commit()
        return row

    def _clear_pending_revenge(self, victim_id: str, stolen_duck_id: str):
        cur = self.db.cursor()
        cur.execute(
            "DELETE FROM pending_revenge WHERE victim_id = ? AND stolen_duck_id = ?",
            (victim_id, stolen_duck_id),
        )
        self.db.commit()

    def _random_battle_flavor(self, winner_name: str, loser_name: str, margin: int) -> str:
        intense = margin >= REVENGE_SWING_STEAL_THRESHOLD
        lines = [
            f"{winner_name} launches a perfectly timed wing combo and outpaces {loser_name}.",
            f"A storm of feathers erupts as {winner_name} overwhelms {loser_name}.",
            f"{winner_name} reads every move, then counters {loser_name} with a final splash.",
            f"{winner_name} circles high, dives hard, and breaks through {loser_name}'s guard.",
            f"{winner_name} ducks low and lands the deciding hit on {loser_name}.",
        ]
        if intense:
            lines.extend(
                [
                    f"{winner_name} absolutely dominates the pond and leaves {loser_name} reeling.",
                    f"{winner_name} unleashes an unstoppable barrage while {loser_name} cannot respond.",
                ]
            )
        return random.choice(lines)

    def _duck_power(self, duck_row: sqlite3.Row, owner_id: str | None = None) -> int:
        base = int(duck_row["attack"]) + int(duck_row["defense"]) + int(duck_row["speed"])
        if owner_id is None:
            return base
        return base + self.items.battle_bonus(owner_id) * 3

    async def _handle_revenge_battle(self, ctx: commands.Context, pending_revenge: sqlite3.Row) -> bool:
        """
        Runs revenge battle flow and returns True if this command execution is fully handled.
        """
        victim_id = str(ctx.author.id)
        thief_id = pending_revenge["thief_id"]
        stolen_duck_id = pending_revenge["stolen_duck_id"]

        if thief_id == victim_id:
            self._clear_pending_revenge(victim_id, stolen_duck_id)
            return False

        victim_member = ctx.guild.get_member(int(victim_id)) if ctx.guild else None
        thief_member = ctx.guild.get_member(int(thief_id)) if ctx.guild else None
        victim_name = victim_member.display_name if victim_member else f"User {victim_id}"
        thief_name = thief_member.display_name if thief_member else f"User {thief_id}"

        victim_fighter_id = self._get_random_duck_from_user(victim_id)
        thief_fighter_id = self._get_random_duck_from_user(thief_id)

        if not victim_fighter_id or not thief_fighter_id:
            self._clear_pending_revenge(victim_id, stolen_duck_id)
            await ctx.reply(
                "⚠️ Revenge window closed: one side has no ducks left to battle with."
            )
            return True

        victim_duck = self._get_duck(victim_fighter_id)
        thief_duck = self._get_duck(thief_fighter_id)
        if not victim_duck or not thief_duck:
            self._clear_pending_revenge(victim_id, stolen_duck_id)
            await ctx.reply("⚠️ Revenge window closed due to missing duck data.")
            return True

        victim_power = self._duck_power(victim_duck, owner_id=victim_id)
        thief_power = self._duck_power(thief_duck, owner_id=thief_id)
        margin = abs(victim_power - thief_power)

        if victim_power == thief_power:
            # Tie-break so the battle always resolves.
            if random.random() < 0.5:
                victim_power += 1
            else:
                thief_power += 1
            margin = abs(victim_power - thief_power)

        victim_wins = victim_power > thief_power
        winner_name = victim_duck["name"] if victim_wins else thief_duck["name"]
        loser_name = thief_duck["name"] if victim_wins else victim_duck["name"]
        flavor = self._random_battle_flavor(winner_name=winner_name, loser_name=loser_name, margin=margin)

        # Revenge trigger is one-time and does not apply cooldown logic.
        self._clear_pending_revenge(victim_id, stolen_duck_id)

        results = []
        stolen_duck = self._get_duck(stolen_duck_id)
        stolen_is_with_thief = stolen_duck and stolen_duck["owner_id"] == thief_id

        if victim_wins and stolen_is_with_thief:
            self._remove_duck_from_user(thief_id, stolen_duck_id)
            self._add_duck_to_user(victim_id, stolen_duck_id)
            self._set_duck_owner(stolen_duck_id, victim_id)
            results.append(f"🦆 {ctx.author.mention} steals back **{stolen_duck['name']}**!")

            if margin >= REVENGE_SWING_STEAL_THRESHOLD:
                bonus_id = self._get_random_stealable_duck_from_user(
                    thief_id,
                    exclude_duck_ids={stolen_duck_id},
                )
                if bonus_id:
                    bonus_duck = self._get_duck(bonus_id)
                    self._remove_duck_from_user(thief_id, bonus_id)
                    self._add_duck_to_user(victim_id, bonus_id)
                    self._set_duck_owner(bonus_id, victim_id)
                    results.append(
                        f"💥 Massive victory (+{margin})! {ctx.author.mention} also steals **{bonus_duck['name']}**."
                    )
        elif victim_wins and not stolen_is_with_thief:
            results.append(
                f"⚠️ {ctx.author.mention} wins the battle, but the original stolen duck is no longer with <@{thief_id}>."
            )
        else:
            results.append("😵 The thief defends successfully. Nothing is stolen back.")
            if margin >= REVENGE_SWING_STEAL_THRESHOLD:
                counter_id = self._get_random_stealable_duck_from_user(victim_id)
                if counter_id:
                    counter_duck = self._get_duck(counter_id)
                    self._remove_duck_from_user(victim_id, counter_id)
                    self._add_duck_to_user(thief_id, counter_id)
                    self._set_duck_owner(counter_id, thief_id)
                    results.append(
                        f"🔥 Brutal defense (+{margin})! <@{thief_id}> steals **{counter_duck['name']}** from {ctx.author.mention}."
                    )

        ctx_lines = self._pvp_weather_effects_lines(
            victim_id,
            thief_id,
            label_a=victim_name,
            label_b=thief_name,
        )
        embed = discord.Embed(
            title="⚔️ Revenge Duck Battle!",
            description=(
                f"{ctx_lines}"
                f"**{victim_name}** sends **{victim_duck['name']}** "
                f"(⚔️ {victim_duck['attack']}  🛡️ {victim_duck['defense']}  💨 {victim_duck['speed']})\n"
                f"vs\n"
                f"**{thief_name}** sends **{thief_duck['name']}** "
                f"(⚔️ {thief_duck['attack']}  🛡️ {thief_duck['defense']}  💨 {thief_duck['speed']})\n\n"
                f"{flavor}"
            ),
            color=discord.Color.red() if victim_wins else discord.Color.dark_red(),
        )
        embed.add_field(name="Outcome", value="\n".join(results), inline=False)
        embed.set_footer(text="This revenge trigger does not consume or refresh cooldowns.")
        await ctx.reply(embed=embed, mention_author=False)
        return True

    def _generate_duck_list_html(self, user: discord.Member) -> Path:
        """Generate dashboard HTML for a user's ducks and return the file path."""
        cur = self.db.cursor()
        row = self._get_user(str(user.id))
        ducks = []
        if row and row["ducks_json"]:
            duck_ids = json.loads(row["ducks_json"])
            if duck_ids:
                q_marks = ",".join("?" for _ in duck_ids)
                cur.execute(f"SELECT * FROM ducks WHERE duck_id IN ({q_marks})", duck_ids)
                ducks = cur.fetchall()

        file_path = HTML_DIR / f"{user.id}.html"
        return generate_duck_dashboard_html(
            user_display_name=user.display_name,
            ducks=ducks,
            output_path=file_path,
        )

    # ---------- COMMANDS ----------
    @commands.command(name="help")
    async def duck_help(self, ctx: commands.Context):
        """Show duck game rules and commands."""
        commands_lines = [
            "`!duck` - Catch a duck (with cooldown).",
            "`!battle` - Your random duck vs another member's (by power); **once per UTC hour** unless you keep **winning** (streak = chain). "
            "Winner takes the loser's fighter only if it's stealable (**not** Shiny / Legendary / Mythic).\n"
            "`!battle @member` - Challenge them; they use `!battle accept` / `!battle deny` within **90s** (only one pending battle per person at a time). "
            "Does **not** use the random `!battle` hourly cooldown.",
            "`!ducks [@member]` - Open your/their duck dashboard.",
            "`!leaderboard` - Show top duck collectors in this server.",
            "`!give @member <duck_name>` - Transfer one of your ducks.",
            "`!release <duck_name>` - Release (delete) one of your ducks.",
            "`!items` / `!weathers` / `!energies` - Explain drops, global weather, and Keish/Zay events.",
            "`!help` - Show this help message.",
        ]
        embed = discord.Embed(
            title="FishinTiffin Duck Game Help",
            description=(
                "Catch ducks, build your collection, and steal your way to the top.\n\n"
                "**How to Play**\n"
                "Use `!duck` to catch a duck per cooldown. Every catch rolls rarity and stats.\n\n"
                "**Rarities**\n"
                "- Tiers go from **Common** to **Mythic**; higher tiers are harder to find.\n\n"
                "**Core Mechanics**\n"
                "- **Cooldown** is randomized after every successful catch.\n"
                "- **Stats** (attack/defense/speed) increase with rarity.\n"
                "- **Shiny** ducks are a rare cosmetic twist.\n"
                "- **Theft**: Sometimes `!duck` pulls a random duck from another player instead of a fresh catch.\n"
                "- If someone steals from you, catching a duck soon after can trigger revenge-steal.\n"
                "- **Keish's ENERGY** & **Zay's ENERGY**: rare `!duck` procs — see **`!energies`**.\n"
                "- **Weather** is server-wide; see **`!weathers`**.\n"
                "- **Items** drop from `!duck` sometimes — see **`!items`**.\n\n"
                "**Commands**\n"
                f"{chr(10).join(commands_lines)}"
            ),
            color=discord.Color.blurple(),
        )
        embed.set_footer(text="FishinTiffin Duck Game")
        await ctx.reply(embed=embed, mention_author=False)

    @commands.command(name="items")
    async def duck_items_info(self, ctx: commands.Context):
        """Explain item drops and effects."""
        embed = discord.Embed(
            title="Items",
            description=(
                "**How you get them**\n"
                "- Sometimes your `!duck` roll is an **item proc**: you receive **one** item at random "
                ". Getting the same item again **refreshes** its timer.\n\n"
                "**Big Guy Protein**\n"
                f"- **+{BIG_GUY_STAT_BONUS}** to **attack**, **defense**, and **speed** when your duck's power is calculated "
                "for **2 hours**.\n\n"
                "**AriPie Energy**\n"
                f"- Your post-catch **cooldown** is divided by **{ARI_PIE_COOLDOWN_DIVISOR}** (stacks with weather), for **30 minutes**.\n\n"
            ),
            color=discord.Color.blurple(),
        )
        embed.set_footer(text="FishinTiffin Duck Game")
        await ctx.reply(embed=embed, mention_author=False)

    @commands.command(name="weathers")
    async def duck_weathers_info(self, ctx: commands.Context):
        """Explain global weather."""
        embed = discord.Embed(
            title="Weather — Global pond conditions",
            description=(
                "**What it is**\n"
                "- **One** weather applies to **everyone** at once. It only changes when someone's `!duck` hits a **weather proc**.\n"
                "- The new weather is picked **at random** from the possibilities that are **not** the current one.\n\n"
                "**Normal**\n"
                "- Default. **No** shiny or cooldown modifiers.\n\n"
                "**Sunshine Sunflowers**\n"
                "- **3×** shiny odds on new catches.\n\n"
                "**Jerm Cloud**\n"
                "- **1.5×** longer **catch cooldowns** for everyone (+50%)."
            ),
            color=WEATHER_EMBED_COLOR,
        )
        embed.set_footer(text="FishinTiffin Duck Game")
        await ctx.reply(embed=embed, mention_author=False)

    @commands.command(name="energies")
    async def duck_energies_info(self, ctx: commands.Context):
        """Explain Keish's and Zay's ENERGY."""
        d1, d2, d3 = ZAY_DEFENSE_PROBS
        pct1, pct2, pct3 = int(round(d1 * 100)), int(round(d2 * 100)), int(round(d3 * 100))
        embed = discord.Embed(
            title="Energies",
            description=(
                "**Keish's ENERGY**\n"
                "- Rare **`!duck` proc**: standalone message (no catch that command).\n"
                f"- You get **{KEISH_FLOCK_ROLLS}** special **`!duck`** pulls: each time, **he** nets a **flock** of "
                "new ducks at once, with **no cooldown** between those pulls.\n"
                "- **Cannot** overlap **Zay's ENERGY** on you (and vice versa).\n"
                "- While **he** still owes you flock pulls, **weather** and **item** procs from `!duck` are skipped.\n\n"
                "**Zay's ENERGY**\n"
                "- Rare **`!duck` proc**: standalone event — he targets **your collection**.\n"
                "- Use **`!duck`** up to **three** times; each use is **one defense round** with its own success chance "
                f"(**{pct1}%**, then **{pct2}%**, then **{pct3}%**).\n"
                "- **The more** you **defend**, the **less** he **steals**.\n"
                "- **Fail once** → **steal finale**: he can remove multiple ducks in one go (scaled to how many he could take at the start). "
                "Only **Legendary** and **Mythic** ducks are **never** taken.\n"
                "- **Win all three** → full-defense finale; you cannot `!battle` until the encounter is over.\n"
                "- Finish Zay (win or lose) before **`!battle`** works again."
            ),
            color=discord.Color.blurple(),
        )
        embed.set_footer(text="FishinTiffin Duck Game")
        await ctx.reply(embed=embed, mention_author=False)

    async def _handle_keish_flock_catch(self, ctx: commands.Context, user_id: str) -> None:
        """One of three Keish flock pulls: 3–7 new ducks, random success art, no outcome roll."""
        n = random.randint(KEISH_FLOCK_MIN, KEISH_FLOCK_MAX)
        ctx_lines = self._duck_catch_weather_effects_lines(user_id)
        lines: list[str] = []
        try:
            for _ in range(n):
                url, ts = await self._fetch_duck_url()
                duck_id = _generate_duck_id()
                _log(f"[duck] keish flock url={url} -> duck_id={duck_id}")
                duck_row = self._create_duck_record(duck_id, url, ts)
                self._add_duck_to_user(user_id, duck_id)
                self._set_duck_owner(duck_id, user_id)
                rarity = duck_row["rarity"]
                shiny = " ✨" if duck_row["shiny"] else ""
                lines.append(f"• **{duck_row['name']}** — {rarity}{shiny}")
        except Exception as e:
            _log(f"[duck] keish flock ERROR: {e}")
            await ctx.reply("He lost the net mid-haul—try **`!duck`** again in a moment.", mention_author=False)
            return

        self.keish.consume_one_roll(user_id)
        remaining = self.keish.flock_rolls_remaining(user_id)
        if remaining > 0:
            status_msg = f"**{remaining}** flock pull(s) left—**`!duck`** again (**no cooldown**)."
        else:
            cd = self._update_cooldown(user_id)
            status_msg = (
                f"His rush is over. Next catch in **{_fmt_duration(cd)}**."
                if cd <= COOLDOWN_MEAN
                else f"His rush is over. You're winded—next catch in **{_fmt_duration(cd)}**."
            )

        duck_word = "duck" if n == 1 else "ducks"
        footer_text = f"**{n}** {duck_word} caught this pull."

        flock_head = "**1** new duck joined your flock:" if n == 1 else f"**{n}** new ducks joined your flock:"
        desc = f"{ctx_lines}{status_msg}\n\n{flock_head}\n" + "\n".join(lines)
        embed = discord.Embed(
            title=KEISH_FLOCK_CATCH_TITLE,
            description=desc,
            color=discord.Color.gold(),
        )
        embed.set_footer(text=footer_text)

        img_path = pick_keish_success_image_path()
        file_obj: discord.File | None = None
        if img_path is not None and img_path.is_file():
            file_obj = discord.File(img_path, filename=img_path.name)
            embed.set_image(url=f"attachment://{img_path.name}")

        try:
            if file_obj is not None:
                await ctx.reply(embed=embed, file=file_obj, mention_author=False)
            else:
                await ctx.reply(embed=embed, mention_author=False)
        except discord.HTTPException as e:
            LOGGER.info("[duck] keish flock reply failed: %s", e)

    @commands.command(name="duck")
    async def duck(self, ctx: commands.Context):
        """
        Catch a duck via the Duck API.
        - Each catch creates or steals a duck; per-user cooldown unless Keish's or Zay's ENERGY is active.
        - Keish proc: announcement + three flock pulls (each 3–7 new ducks, random success art, no cooldown between pulls).
        - Zay proc: standalone event (no duck this command); assets/zay_energy/zay_energy.gif; up to three !duck defenses; clash / finale assets in that folder.
        - Announces catch + attributes (+ theft mention) and cooldown message.
        """
        try:
            pending_revenge = self._get_pending_revenge(str(ctx.author.id))
            if pending_revenge:
                handled = await self._handle_revenge_battle(ctx, pending_revenge)
                if handled:
                    return

            new_owner_id = str(ctx.author.id)
            if self.zay.active(new_owner_id):
                await self.zay.handle_defense_attempt(ctx, new_owner_id)
                return

            # 1) cooldown pre-check
            on_cd, remaining = self._check_on_cooldown(str(ctx.author.id))
            if on_cd:
                await ctx.reply(f"⏳ You’re still recovering! Next catch in **{_fmt_duration(remaining)}**.")
                return

            if self.keish.active(new_owner_id):
                await self._handle_keish_flock_catch(ctx, new_owner_id)
                return

            allow_zay_proc = (
                ctx.guild
                and not self.zay.active(new_owner_id)
                and not self.keish.active(new_owner_id)
            )
            allow_keish_proc = not self.zay.active(new_owner_id) and not self.keish.active(new_owner_id)
            # Keish flock pulls return above while active, so weather/item/boot are never suppressed here.
            outcome = _roll_duck_outcome(
                allow_zay_proc=bool(allow_zay_proc),
                allow_keish_proc=allow_keish_proc,
                allow_weather_proc=True,
                allow_item_proc=True,
                allow_boot=True,
            )

            # Zay's ENERGY proc: standalone event (no boot/catch/steal this command), like Keish's own card + GIF.
            if outcome == "zay_proc":
                self.zay.start(new_owner_id, ctx.guild.id, ctx.channel.id)
                embed = discord.Embed(
                    title=ZAY_PROC_TITLE,
                    description=zay_proc_description(),
                    color=discord.Color.dark_purple(),
                )
                zay_energy_file = None
                if ZAY_ENERGY_GIF_PATH.is_file():
                    zay_energy_file = discord.File(ZAY_ENERGY_GIF_PATH, filename="zay_energy.gif")
                    embed.set_image(url="attachment://zay_energy.gif")
                embed.set_footer(text=zay_proc_footer())
                if zay_energy_file:
                    await ctx.reply(embed=embed, file=zay_energy_file, mention_author=False)
                else:
                    await ctx.reply(embed=embed, mention_author=False)
                return

            # Keish's ENERGY proc: standalone event (no boot/catch/steal this command).
            if outcome == "keish_proc":
                self.keish.grant(new_owner_id)
                embed = discord.Embed(
                    title=KEISH_PROC_TITLE,
                    description=keish_proc_description(),
                    color=discord.Color.gold(),
                )
                keish_energy_file = None
                if KEISH_ENERGY_GIF_PATH.is_file():
                    keish_energy_file = discord.File(KEISH_ENERGY_GIF_PATH, filename="keish_energy.gif")
                    embed.set_image(url="attachment://keish_energy.gif")
                embed.set_footer(text="**0** ducks caught — **`!duck`** runs each flock pull.")
                if keish_energy_file:
                    await ctx.reply(embed=embed, file=keish_energy_file, mention_author=False)
                else:
                    await ctx.reply(embed=embed, mention_author=False)
                return

            if outcome == "weather_proc":
                new_weather = self.weather.pick_new()
                embed, file_obj = self.weather.build_proc_embed(new_weather)
                if file_obj is not None:
                    await ctx.reply(embed=embed, file=file_obj, mention_author=False)
                else:
                    await ctx.reply(embed=embed, mention_author=False)
                return

            if outcome == "item_proc":
                new_item = self.items.grant_random(new_owner_id)
                embed, file_obj = self.items.build_proc_embed(new_item)
                if file_obj is not None:
                    await ctx.reply(embed=embed, file=file_obj, mention_author=False)
                else:
                    await ctx.reply(embed=embed, mention_author=False)
                return

            theft_text = ""

            # 2) Boot outcome (does not count as a backend catch)
            if outcome == "boot":
                boot_flair = random.choice(
                    [
                        "You yank your line up triumphantly... and it's an old boot. The silence is deafening.",
                        "You pose like a champion angler, then realize you're holding a soggy boot.",
                        "You reel it in with confidence, only to discover pure footwear disappointment.",
                        "You expected feathers and glory; you got a boot and secondhand embarrassment.",
                        "You stare at the catch, then back at chat. Nobody needs to say anything.",
                    ]
                )
                embed = discord.Embed(
                    title="Congrats on your new *duck*...?",
                    description=(
                        f"{boot_flair}\n\n"
                        "**Name:** Old Boot\n"
                        "**Rarity:** Trash\n"
                        "**Stats:** ⚔️ 0  🛡️ 0  💨 0\n"
                    ),
                    color=discord.Color.dark_grey(),
                )
                embed.set_image(url=_pick_boot_image_url())
                embed.set_footer(text="No duck was actually caught. Try again.")
                await ctx.reply(embed=embed, mention_author=False)
                return

            # 3) Steal outcome vs fresh new duck
            will_steal = outcome == "steal"
            steal_result = None
            if will_steal:
                allowed_ids = await self._guild_member_ids(ctx.guild)
                steal_result = self._get_random_user_with_stealable_ducks(
                    exclude_user_id=new_owner_id,
                    allowed_user_ids=allowed_ids,
                )
            
            if will_steal and steal_result:
                # Steal a random duck from a random user
                prev_owner_id, duck_id = steal_result
                cur = self.db.cursor()
                cur.execute("SELECT * FROM ducks WHERE duck_id = ?", (duck_id,))
                duck_row = cur.fetchone()
                
                if duck_row:
                    # Transfer ownership
                    self._remove_duck_from_user(prev_owner_id, duck_id)
                    self._add_duck_to_user(new_owner_id, duck_id)
                    self._set_duck_owner(duck_id, new_owner_id)
                    # Victim gets immediate !duck access for revenge.
                    self._refresh_user_cooldown(prev_owner_id)
                    victim_m = ctx.guild.get_member(int(prev_owner_id)) if ctx.guild else None
                    thief_m = ctx.guild.get_member(int(new_owner_id)) if ctx.guild else None
                    victim_dn = victim_m.display_name if victim_m else f"User {prev_owner_id}"
                    thief_dn = thief_m.display_name if thief_m else f"User {new_owner_id}"
                    theft_text = (
                        f"\n⚠️ **{victim_dn}**'s duck **{duck_row['name']}** was stolen by **{thief_dn}**.\n"
                        f"🌀 **{victim_dn}**'s catch cooldown was refreshed. **{victim_dn}** can use `!duck` within "
                        "**5 minutes** to trigger revenge."
                    )
                    self._set_pending_revenge(
                        victim_id=prev_owner_id,
                        thief_id=new_owner_id,
                        stolen_duck_id=duck_id,
                    )
                else:
                    # Fallback: create new duck if stolen duck doesn't exist (shouldn't happen)
                    will_steal = False
            
            if not will_steal or not steal_result:
                # Create a new duck
                url, ts = await self._fetch_duck_url()
                duck_id = _generate_duck_id()
                _log(f"[duck] API url={url} -> new duck_id={duck_id} ts={ts}")
                duck_row = self._create_duck_record(duck_id, url, ts)
                # Add to new owner
                self._add_duck_to_user(new_owner_id, duck_id)
                self._set_duck_owner(duck_id, new_owner_id)

            # 4) Cooldown after a normal catch (Keish flock pulls handle their own cooldown)
            cd = self._update_cooldown(new_owner_id)
            cd_msg = (
                f"Your energy is preserved! Next catch available in: **{_fmt_duration(cd)}**."
                if cd <= COOLDOWN_MEAN
                else f"You're feeling a bit tired... Next catch available in: **{_fmt_duration(cd)}**."
            )

            # 5) flavor + attributes message
            rarity = duck_row["rarity"]
            name = duck_row["name"]
            shiny = bool(duck_row["shiny"])
            atk, dfs, spd = duck_row["attack"], duck_row["defense"], duck_row["speed"]

            flair = RARITY_CATCH_FLAIR[rarity]
            ctx_lines = self._duck_catch_weather_effects_lines(new_owner_id)

            color = RARITY_EMBED_COLORS.get(rarity, discord.Color.dark_grey())
            title = (
                f"{'🌟✨ Shiny Duck Appeared! ✨🌟 ' if shiny else ''}"
                f"Congrats on your new duck!{' SO SHINY!' if shiny else ''}"
            )
            catch_body = (
                f"{flair}\n\n"
                f"{ctx_lines}"
                f"**Name:** {name}\n"
                f"**Rarity:** {rarity}{' ✨' if shiny else ''}\n"
                f"**Stats:** ⚔️ {atk}  🛡️ {dfs}  💨 {spd}\n"
            )

            embed = discord.Embed(
                title=title,
                description=catch_body,
                color=color,
            )
            embed.set_image(url=duck_row["url"])

            embed.set_footer(text=cd_msg)

            catch_message = await ctx.reply(embed=embed, mention_author=False)
            if rarity in ("Legendary", "Mythic"):
                try:
                    await catch_message.add_reaction("👑")
                except discord.HTTPException:
                    pass
            if theft_text:
                await ctx.send(theft_text)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            _log(f"[duck] ERROR: {e}")
            await ctx.reply("Something went wrong catching your duck. Please try again in a moment.")

    @commands.command(name="ducks")
    async def ducks_list(self, ctx: commands.Context, member: discord.Member | None = None):
        """List ducks owned by you or another member."""
        target = member or ctx.author

        # Always regenerate HTML for this user
        self._generate_duck_list_html(target)

        # Count ducks for quick display
        row = self._get_user(str(target.id))
        count = len(json.loads(row["ducks_json"])) if row and row["ducks_json"] else 0

        # Point to API redirect route (short URL)
        url = f"{self.dashboard_base_url}/{target.id}"

        await ctx.reply(
            f"{target.display_name} owns {count} duck{'s' if count != 1 else ''} 🦆\n"
            f"View them here: [Duck Dashboard]({url})"
        )

    @commands.command(name="leaderboard")
    async def duck_leaderboard(self, ctx: commands.Context):
        """Show the top 10 duck collectors among members currently in this server."""
        if ctx.guild is None:
            await ctx.reply("Use this command in a server to see the leaderboard.")
            return

        member_ids = await self._guild_member_ids(ctx.guild) or set()

        cur = self.db.cursor()
        cur.execute("SELECT user_id, ducks_json FROM users")
        rows = cur.fetchall()

        leaderboard = []
        total_ducks = 0
        for row in rows:
            duck_ids = json.loads(row["ducks_json"]) if row["ducks_json"] else []
            count = len(duck_ids)
            total_ducks += count
            leaderboard.append((row["user_id"], count))

        # Sort descending by count, then keep only users still in the guild (ranks compress)
        leaderboard.sort(key=lambda x: x[1], reverse=True)
        eligible = [
            (user_id, count)
            for user_id, count in leaderboard
            if user_id in member_ids
        ]
        top10 = eligible[:10]

        if not top10:
            if not leaderboard:
                await ctx.reply("No ducks have been caught yet!")
            else:
                await ctx.reply("No collectors are currently in this server.")
            return

        # Build lines with medals for top 3
        medals = {1: "🥇", 2: "🥈", 3: "🥉"}
        lines = []
        for i, (user_id, count) in enumerate(top10, start=1):
            member = ctx.guild.get_member(int(user_id))
            medal = medals.get(i, f"{i}.")
            duck_label = f"{count} duck{'s' if count != 1 else ''}"
            lines.append(f"{medal} **{member.display_name}** — {duck_label}")

        embed = discord.Embed(
            title="🏆 Duck Leaderboard — PreSeason 🏆",
            description="\n".join(lines),
            color=discord.Color.gold()
        )
        embed.set_footer(text=f"Total ducks caught in the server: {total_ducks}")

        await ctx.reply(embed=embed)

    async def _handle_duck_role_reaction(
        self,
        payload: discord.RawReactionActionEvent,
        *,
        added: bool,
    ) -> None:
        if payload.guild_id is None or payload.user_id == self.bot.user.id:
            return
        if str(payload.emoji) != DUCK_ROLE_EMOJI:
            return

        configured_roles_channel = getattr(self.bot, "roles_channel", None)
        if configured_roles_channel is not None and payload.channel_id != configured_roles_channel:
            return

        configured_role_id = getattr(self.bot, "duck_role", None)
        if configured_role_id is None:
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

        role = guild.get_role(configured_role_id)
        if role is None:
            return

        try:
            member = guild.get_member(payload.user_id) or await guild.fetch_member(payload.user_id)
        except discord.HTTPException:
            return
        if member.bot:
            return

        try:
            if added:
                if role in member.roles:
                    return
                await member.add_roles(role, reason="Reacted to duck onboarding role message")
            else:
                if role not in member.roles:
                    return
                await member.remove_roles(role, reason="Removed reaction from duck onboarding role message")
        except discord.Forbidden:
            action = "add" if added else "remove"
            _log(
                f"[duck-role] Missing permission to {action} role {configured_role_id} in guild {payload.guild_id}"
            )
        except discord.HTTPException:
            action = "to" if added else "from"
            verb = "add" if added else "remove"
            _log(f"[duck-role] Failed to {verb} role {configured_role_id} {action} user {payload.user_id}")

    @commands.command(name="give")
    async def give_duck(self, ctx: commands.Context, member: discord.Member, *, duck_name: str):
        """
        Give one of your ducks to another member.
        Usage: !give @user DuckName
        """
        giver_id = str(ctx.author.id)
        receiver_id = str(member.id)

        if giver_id == receiver_id:
            await ctx.reply("You can’t give a duck to yourself!")
            return

        row = self._get_owned_duck_row_by_name(giver_id, duck_name)
        if not row:
            cur = self.db.cursor()
            cur.execute("SELECT 1 FROM ducks WHERE name = ? LIMIT 1", (duck_name,))
            if cur.fetchone():
                await ctx.reply(f"You don't have a duck named **{duck_name}** to give.")
            else:
                await ctx.reply(f"No duck named **{duck_name}** was found.")
            return

        # Remove from giver
        self._remove_duck_from_user(giver_id, row["duck_id"])

        # Add to receiver
        self._add_duck_to_user(receiver_id, row["duck_id"])
        self._set_duck_owner(row["duck_id"], receiver_id)

        # Confirm
        await ctx.reply(
            f"🎁 {ctx.author.mention} gave **{duck_name}** ({row['rarity']}) "
            f"to {member.mention}!"
        )

    def _clear_pending_battle_challenge(self, pending: _PendingBattleChallenge) -> bool:
        key_d = (pending.guild_id, pending.defender_id)
        key_c = (pending.guild_id, pending.challenger_id)
        if self._pending_battle_by_defender.get(key_d) is not pending:
            return False
        del self._pending_battle_by_defender[key_d]
        del self._pending_battle_by_challenger[key_c]
        if pending.timeout_task and not pending.timeout_task.done():
            pending.timeout_task.cancel()
        return True

    async def _pending_battle_timeout_worker(self, pending: _PendingBattleChallenge) -> None:
        try:
            await asyncio.sleep(BATTLE_CHALLENGE_TIMEOUT_SECONDS)
        except asyncio.CancelledError:
            return
        if not self._clear_pending_battle_challenge(pending):
            return
        channel = self.bot.get_channel(pending.channel_id)
        if channel is None:
            try:
                ch = await self.bot.fetch_channel(pending.channel_id)
            except discord.HTTPException:
                ch = None
            channel = ch
        if isinstance(channel, discord.abc.Messageable):
            try:
                await channel.send(
                    f"⏱️ Battle challenge timed out ({BATTLE_CHALLENGE_TIMEOUT_SECONDS}s): "
                    f"<@{pending.challenger_id}> vs <@{pending.defender_id}>."
                )
            except discord.HTTPException:
                pass

    async def _run_duck_battle_fight(
        self,
        ctx: commands.Context,
        challenger_id: str,
        opponent_id: str,
        *,
        challenger_member: discord.Member | None = None,
        uses_ranked_battle_cooldown: bool = True,
    ) -> None:
        """Resolve PvP: random fighter from each side; challenger is ``challenger_id``.

        When ``uses_ranked_battle_cooldown`` is True, updates the challenger's UTC-hour `!battle` bucket / streak.
        Targeted accepts pass False so random `!battle` limits stay separate.
        """
        challenger_duck_id = self._get_random_duck_from_user(challenger_id)
        opponent_duck_id = self._get_random_duck_from_user(opponent_id)
        if not challenger_duck_id or not opponent_duck_id:
            await ctx.reply("Battle aborted: one side has no ducks left to battle with.")
            return

        challenger_duck = self._get_duck(challenger_duck_id)
        opponent_duck = self._get_duck(opponent_duck_id)
        if not challenger_duck or not opponent_duck:
            await ctx.reply("Battle aborted: missing duck data.")
            return

        guild = ctx.guild
        challenger_name = (
            challenger_member.display_name
            if challenger_member is not None
            else (
                guild.get_member(int(challenger_id)).display_name
                if guild and guild.get_member(int(challenger_id))
                else f"User {challenger_id}"
            )
        )
        opponent_m = guild.get_member(int(opponent_id)) if guild else None
        opponent_name = (
            opponent_m.display_name if opponent_m else f"User {opponent_id}"
        )

        cp = self._duck_power(challenger_duck, owner_id=challenger_id)
        op = self._duck_power(opponent_duck, owner_id=opponent_id)
        if cp == op:
            if random.random() < 0.5:
                cp += 1
            else:
                op += 1
        margin = abs(cp - op)

        challenger_wins = cp > op
        winner_name = challenger_duck["name"] if challenger_wins else opponent_duck["name"]
        loser_name = opponent_duck["name"] if challenger_wins else challenger_duck["name"]
        flavor = self._random_battle_flavor(winner_name=winner_name, loser_name=loser_name, margin=margin)

        bucket = self._battle_hour_bucket_now() if uses_ranked_battle_cooldown else None
        if challenger_wins:
            if uses_ranked_battle_cooldown and bucket is not None:
                self._set_battle_state(challenger_id, bucket, streak=True)
            if self._is_stealable_duck(opponent_duck):
                self._remove_duck_from_user(opponent_id, opponent_duck_id)
                self._add_duck_to_user(challenger_id, opponent_duck_id)
                self._set_duck_owner(opponent_duck_id, challenger_id)
                outcome_line = (
                    f"🏆 **{challenger_name}** wins! **{challenger_name}** takes **{opponent_duck['name']}** "
                    f"from **{opponent_name}**."
                )
            else:
                outcome_line = (
                    f"🏆 **{challenger_name}** wins the fight! **{opponent_duck['name']}** is "
                    f"**Shiny**, **Legendary**, or **Mythic** — it stays with **{opponent_name}**."
                )
            title = "⚔️ Duck Battle — Victory!"
            color = discord.Color.green()
            if uses_ranked_battle_cooldown:
                footer = (
                    "You won — `!battle` again now to keep rolling, or stop anytime. "
                    "One battle per hour, win streaks keep you alive!"
                )
            else:
                footer = "Targeted battle — does not affect the random `!battle` hourly limit."
        else:
            if uses_ranked_battle_cooldown and bucket is not None:
                self._set_battle_state(challenger_id, bucket, streak=False)
            if self._is_stealable_duck(challenger_duck):
                self._remove_duck_from_user(challenger_id, challenger_duck_id)
                self._add_duck_to_user(opponent_id, challenger_duck_id)
                self._set_duck_owner(challenger_duck_id, opponent_id)
                outcome_line = (
                    f"💔 **{opponent_name}** wins! **{opponent_name}** takes **{challenger_duck['name']}** "
                    f"from **{challenger_name}**."
                )
            else:
                outcome_line = (
                    f"💔 **{opponent_name}** wins the fight! **{challenger_duck['name']}** is protected "
                    f"(Shiny / Legendary / Mythic) and stays with **{challenger_name}**."
                )
            title = "⚔️ Duck Battle — Defeat"
            color = discord.Color.red()
            if uses_ranked_battle_cooldown:
                footer = (
                    "One battle per hour, win streaks keep you alive! "
                    "`!battle` again after a win, or wait for the next hour if you lost."
                )
            else:
                footer = "Targeted battle — does not affect the random `!battle` hourly limit."

        battle_ctx = self._pvp_weather_effects_lines(
            challenger_id,
            opponent_id,
            label_a=challenger_name,
            label_b=opponent_name,
        )
        description = (
            f"{battle_ctx}"
            f"**{challenger_name}** sends **{challenger_duck['name']}** "
            f"(⚔️ {challenger_duck['attack']}  🛡️ {challenger_duck['defense']}  💨 {challenger_duck['speed']})\n"
            f"vs\n"
            f"**{opponent_name}** sends **{opponent_duck['name']}** "
            f"(⚔️ {opponent_duck['attack']}  🛡️ {opponent_duck['defense']}  💨 {opponent_duck['speed']})\n\n"
            f"{flavor}\n\n"
            f"{outcome_line}"
        )

        embed = discord.Embed(title=title, description=description, color=color)
        embed.set_footer(text=footer)
        await ctx.reply(embed=embed)

    async def _battle_random(self, ctx: commands.Context) -> None:
        if ctx.guild is None:
            await ctx.reply("Battles can only be used in a server.", mention_author=True)
            return

        user_id = str(ctx.author.id)

        if self.zay.active(user_id):
            await ctx.reply("⚠️ Finish **Zay's ENERGY** with `!duck` before battling.")
            return

        allowed, wait_sec = self._can_battle(user_id)
        if not allowed and wait_sec is not None:
            await ctx.reply(
                f"⏳ You've used your battle for this hour. Next window in **{_fmt_duration(wait_sec)}**."
            )
            return

        challenger_duck_id = self._get_random_duck_from_user(user_id)
        if not challenger_duck_id:
            await ctx.reply("You need at least one duck to battle with.")
            return

        allowed_ids = await self._guild_member_ids(ctx.guild)
        opp = self._get_random_user_with_ducks(
            exclude_user_id=user_id,
            allowed_user_ids=allowed_ids,
        )
        if not opp:
            await ctx.reply("Nobody else in this server has a duck you can battle against (yet).")
            return

        opponent_id, _ = opp
        await self._run_duck_battle_fight(
            ctx,
            user_id,
            opponent_id,
            challenger_member=ctx.author,
        )

    async def _battle_issue_challenge(self, ctx: commands.Context, opponent: discord.Member) -> None:
        if ctx.guild is None:
            await ctx.reply("Battles can only be used in a server.", mention_author=True)
            return

        challenger_id = str(ctx.author.id)
        defender_id = str(opponent.id)

        if challenger_id == defender_id:
            await ctx.reply("You can't battle yourself.")
            return
        if opponent.bot:
            await ctx.reply("Pick a human server member to challenge.")
            return

        if self.zay.active(challenger_id):
            await ctx.reply("⚠️ Finish **Zay's ENERGY** with `!duck` before battling.")
            return

        if not self._get_random_duck_from_user(challenger_id):
            await ctx.reply("You need at least one duck to battle with.")
            return
        if not self._get_random_duck_from_user(defender_id):
            await ctx.reply(f"{opponent.mention} doesn't have any ducks to battle with.")
            return

        guild_id = ctx.guild.id
        if (guild_id, defender_id) in self._pending_battle_by_defender:
            await ctx.reply(
                f"{opponent.mention} already has a pending battle to answer. Try again later."
            )
            return
        if (guild_id, challenger_id) in self._pending_battle_by_challenger:
            await ctx.reply(
                "You already have a pending battle challenge. Wait for accept, deny, or timeout."
            )
            return

        allowed_ids = await self._guild_member_ids(ctx.guild)
        if allowed_ids is not None:
            if defender_id not in allowed_ids:
                await ctx.reply("That member isn't in this server (or member list isn't loaded yet).")
                return
            if challenger_id not in allowed_ids:
                await ctx.reply("Battle aborted: couldn't verify you're in this server.")
                return

        pending = _PendingBattleChallenge(
            guild_id=guild_id,
            challenger_id=challenger_id,
            defender_id=defender_id,
            channel_id=ctx.channel.id,
        )
        self._pending_battle_by_defender[(guild_id, defender_id)] = pending
        self._pending_battle_by_challenger[(guild_id, challenger_id)] = pending
        pending.timeout_task = asyncio.create_task(self._pending_battle_timeout_worker(pending))

        await ctx.reply(
            f"⚔️ {ctx.author.mention} challenges {opponent.mention} to a duck battle!\n"
            f"{opponent.mention}: type `!battle accept` or `!battle deny` within "
            f"**{BATTLE_CHALLENGE_TIMEOUT_SECONDS}** seconds."
        )

    @commands.group(name="battle", invoke_without_command=True)
    async def duck_battle(self, ctx: commands.Context, opponent: discord.Member | None = None):
        """Random PvP battle, or `!battle @member` to challenge (they accept/deny within 90s).

        Winner takes the loser's fighter only if it could be stolen (!duck theft rules: not Shiny / Legendary / Mythic).
        Random `!battle` uses the challenger's UTC-hour limit + win streak; targeted battles do not.
        """
        if ctx.invoked_subcommand is not None:
            return
        if opponent is None:
            await self._battle_random(ctx)
        else:
            await self._battle_issue_challenge(ctx, opponent)

    @duck_battle.command(name="accept")
    async def duck_battle_accept(self, ctx: commands.Context):
        """Accept a pending duck battle challenge (defender only)."""
        if ctx.guild is None:
            await ctx.reply("Battles can only be used in a server.", mention_author=True)
            return

        defender_id = str(ctx.author.id)
        guild_id = ctx.guild.id
        pending = self._pending_battle_by_defender.get((guild_id, defender_id))
        if pending is None:
            await ctx.reply("You don't have a pending battle to accept.")
            return

        challenger_id = pending.challenger_id
        if not self._clear_pending_battle_challenge(pending):
            await ctx.reply("That challenge is no longer active.")
            return

        challenger = ctx.guild.get_member(int(challenger_id))
        if challenger is None:
            try:
                challenger = await ctx.guild.fetch_member(int(challenger_id))
            except discord.HTTPException:
                challenger = None
        if challenger is None:
            await ctx.reply("Battle aborted: challenger is no longer in this server.")
            return

        if self.zay.active(challenger_id):
            await ctx.reply("⚠️ Challenger must finish **Zay's ENERGY** with `!duck` before this battle can run.")
            return

        if not self._get_random_duck_from_user(challenger_id):
            await ctx.reply("Battle aborted: challenger has no ducks left.")
            return
        if not self._get_random_duck_from_user(defender_id):
            await ctx.reply("Battle aborted: you have no ducks left.")
            return

        allowed_ids = await self._guild_member_ids(ctx.guild)
        if allowed_ids is not None:
            if defender_id not in allowed_ids or challenger_id not in allowed_ids:
                await ctx.reply("Battle aborted: member list couldn't be verified.")
                return

        await self._run_duck_battle_fight(
            ctx,
            challenger_id,
            defender_id,
            challenger_member=challenger,
            uses_ranked_battle_cooldown=False,
        )

    @duck_battle.command(name="deny")
    async def duck_battle_deny(self, ctx: commands.Context):
        """Decline a pending duck battle challenge (defender only)."""
        if ctx.guild is None:
            await ctx.reply("Battles can only be used in a server.", mention_author=True)
            return

        defender_id = str(ctx.author.id)
        guild_id = ctx.guild.id
        pending = self._pending_battle_by_defender.get((guild_id, defender_id))
        if pending is None:
            await ctx.reply("You don't have a pending battle to deny.")
            return

        if not self._clear_pending_battle_challenge(pending):
            await ctx.reply("That challenge is no longer active.")
            return

        await ctx.reply(
            f"🙅 {ctx.author.mention} declined the battle from <@{pending.challenger_id}>."
        )

    @commands.command(name="release")
    async def release_duck(self, ctx: commands.Context, *, duck_name: str):
        """Release (delete) your most recently caught duck with this name.

        Usage: !release DuckName
        """
        want = duck_name.strip()
        if not want:
            await ctx.reply("Provide a duck name.")
            return

        user_id = str(ctx.author.id)
        urow = self._get_user(user_id)
        if not urow or not urow["ducks_json"]:
            await ctx.reply("You don't have any ducks to release.")
            return

        duck_ids = json.loads(urow["ducks_json"])
        candidates: list[sqlite3.Row] = []
        for duck_id in duck_ids:
            d = self._get_duck(duck_id)
            if d and d["name"] == want:
                candidates.append(d)

        if not candidates:
            cur = self.db.cursor()
            cur.execute("SELECT 1 FROM ducks WHERE name = ? LIMIT 1", (want,))
            if cur.fetchone():
                await ctx.reply(f"You don't have a duck named **{want}** to release.")
            else:
                await ctx.reply(f"No duck named **{want}** was found.")
            return

        chosen = max(candidates, key=lambda d: int(d["timestamp"]))
        name_out = chosen["name"]
        rarity_out = chosen["rarity"]
        self._obliterate_duck(user_id, chosen["duck_id"])

        embed = discord.Embed(
            title="🕊️ Duck Released",
            description=f"You released **{name_out}** ({rarity_out}).",
            color=RARITY_EMBED_COLORS.get(rarity_out, discord.Color.dark_grey()),
        )
        embed.set_footer(text="One duck lost to the wild — forever.")
        await ctx.reply(embed=embed)

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        await self._handle_duck_role_reaction(payload, added=True)

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent):
        await self._handle_duck_role_reaction(payload, added=False)
