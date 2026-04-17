# duck_manager.py — Duck game cog (!duck, DB, Keish/Zay energy).
from discord.ext import commands
from datetime import datetime, timezone
import discord
import aiohttp
import asyncio
import sqlite3
import json
import random
import uuid
from pathlib import Path
import logging

from .duck_dashboard_html import generate_duck_dashboard_html
from .duck_clock import utc_ts
from .keish_energy import (
    BLESSING_DISPLAY_SECONDS,
    KEISH_ENERGY_GIF_PATH,
    KEISH_PROC_TITLE,
    KEISH_SADDLE_TITLE,
    KEISH_SNAG_TITLE,
    KeishEnergy,
    blessing_proc_description,
)
from .paths import ASSETS_DIR, DUCK_DATA_DIR, HTML_DIR
from .zay_energy import (
    ZAY_ENERGY_GIF_PATH,
    ZAY_PROC_TITLE,
    ZayEnergy,
    zay_proc_description,
    zay_proc_footer,
)

LOGGER = logging.getLogger("fishin_tiffin.ducks")
DUCK_ROLE_EMOJI = "🦆"


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
    ("boot", 3),
    ("steal", 15),
    ("new_duck", 80),
]

# Cooldown parameters
COOLDOWN_MEAN = 90.0  # seconds
COOLDOWN_STD = 360.0   # seconds
COOLDOWN_MIN = 1      # seconds
COOLDOWN_MAX = 15 * 60  # 15 minutes

REVENGE_WINDOW_SECONDS = 5 * 60
REVENGE_SWING_STEAL_THRESHOLD = 20

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

def _roll_shiny() -> bool:
    return random.uniform(0, 100) < SHINY_PROB


def _roll_duck_outcome(*, allow_zay_proc: bool, allow_keish_proc: bool, allow_boot: bool) -> str:
    labels = []
    weights = []
    for label, weight in DUCK_OUTCOME_WEIGHTS:
        if label == "zay_proc" and not allow_zay_proc:
            continue
        if label == "keish_proc" and not allow_keish_proc:
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


def _fmt_age_ago(timestamp: int) -> str:
    age_seconds = int(datetime.now(timezone.utc).timestamp()) - int(timestamp)
    minutes, seconds = divmod(age_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    if days > 0:
        return f"{days}d {hours}h ago"
    if hours > 0:
        return f"{hours}h {minutes}m ago"
    if minutes > 0:
        return f"{minutes}m {seconds}s ago"
    return f"{seconds}s ago"

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
            "- If **Keish** shows up, she's **giving you a boost**—spam **`!duck`** to keep the energy going.\n\n"
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
        self._init_db()
        _log("[DuckManager] initialized")

    async def cog_check(self, ctx: commands.Context) -> bool:
        configured_server = getattr(self.bot, "server", None)
        if configured_server is None or ctx.guild is None:
            return True
        return ctx.guild.id == configured_server

    def cog_unload(self):
        self.zay.cancel_all_tasks()
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
        self.db.commit()

    # ---------- HELPERS ----------
    def _get_user(self, user_id: str) -> sqlite3.Row | None:
        cur = self.db.cursor()
        cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        return cur.fetchone()

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
        shiny = 1 if _roll_shiny() else 0
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

    def _get_random_user_with_stealable_ducks(self, exclude_user_id: str = None) -> tuple[str, str] | None:
        """
        Returns a random (user_id, duck_id) tuple from a user who has stealable ducks.
        exclude_user_id: user to exclude from selection
        Returns None if no users with stealable ducks exist (other than excluded user).
        """
        cur = self.db.cursor()
        cur.execute("SELECT user_id, ducks_json FROM users WHERE ducks_json IS NOT NULL AND ducks_json != '[]' AND ducks_json != ''")
        rows = cur.fetchall()
        
        # Filter out excluded user and collect users with ducks
        candidates = []
        for row in rows:
            user_id = row["user_id"]
            if exclude_user_id and user_id == exclude_user_id:
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
        
        # Pick random user
        random_user_id, duck_ids = random.choice(candidates)
        # Pick random duck from that user
        random_duck_id = random.choice(duck_ids)
        return (random_user_id, random_duck_id)

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

    def _duck_power(self, duck_row: sqlite3.Row) -> int:
        return int(duck_row["attack"]) + int(duck_row["defense"]) + int(duck_row["speed"])

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

        victim_fighter_id = self._get_random_stealable_duck_from_user(victim_id)
        thief_fighter_id = self._get_random_stealable_duck_from_user(thief_id)

        if not victim_fighter_id or not thief_fighter_id:
            self._clear_pending_revenge(victim_id, stolen_duck_id)
            await ctx.reply(
                "⚠️ Revenge window closed: one side has no stealable ducks left to battle with."
            )
            return True

        victim_duck = self._get_duck(victim_fighter_id)
        thief_duck = self._get_duck(thief_fighter_id)
        if not victim_duck or not thief_duck:
            self._clear_pending_revenge(victim_id, stolen_duck_id)
            await ctx.reply("⚠️ Revenge window closed due to missing duck data.")
            return True

        victim_power = self._duck_power(victim_duck)
        thief_power = self._duck_power(thief_duck)
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

        embed = discord.Embed(
            title="⚔️ Revenge Duck Battle!",
            description=(
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
            "`!ducks [@member]` - Open your/their duck dashboard.",
            "`!leaderboard` - Show top duck collectors.",
            "`!showcase <duck_name>` - View a duck's details.",
            "`!give @member <duck_name>` - Transfer one of your ducks.",
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
                "- **Keish's ENERGY**: a rare burst where `!duck` ignores cooldown for a short time "
                "(see the in-channel card; can't overlap **Zay's ENERGY**).\n"
                "- **Zay's ENERGY**: a rare clash where **Zay tries to snatch ducks** from your collection. "
                "Use **`!duck`** up to **three times** to defend—**each roll either holds him off or he grabs his take and leaves**. "
                "Only non–Legendary / non–Mythic ducks can be lost.\n\n"
                "**Commands**\n"
                f"{chr(10).join(commands_lines)}"
            ),
            color=discord.Color.blurple(),
        )
        embed.set_footer(text="Tip: Use !ducks to track your full collection.")
        await ctx.reply(embed=embed, mention_author=False)

    @commands.command(name="duck")
    async def duck(self, ctx: commands.Context):
        """
        Catch a duck via the Duck API.
        - Each catch creates or steals a duck; per-user cooldown unless Keish's or Zay's ENERGY is active.
        - Keish proc: announcement embed + keish_energy.gif; windowed snag/saddle titles on further catches.
        - Zay proc: standalone event (no duck this command); zay_energy.gif; up to three !duck defenses; clash / finale assets.
        - Announces catch + attributes (+ theft mention) and cooldown message.
        """
        # Restrict execution to the ducks channel
        if ctx.channel.id != self.bot.ducks:
            return await ctx.reply(
                "This command can only be used in the 🦆 ducks channel.", delete_after=5
            )
        
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

            allow_zay_proc = (
                ctx.guild
                and not self.zay.active(new_owner_id)
                and not self.keish.active(new_owner_id)
            )
            allow_keish_proc = not self.zay.active(new_owner_id) and not self.keish.active(new_owner_id)
            keish_energy_now = self.keish.active(new_owner_id)
            outcome = _roll_duck_outcome(
                allow_zay_proc=bool(allow_zay_proc),
                allow_keish_proc=allow_keish_proc,
                allow_boot=not keish_energy_now,
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
                cd_msg = (
                    f"✨ **Keish's ENERGY** lasts **{BLESSING_DISPLAY_SECONDS} seconds**—`!duck` ignores cooldown until it fades. "
                    "Your next catch after that sets a normal cooldown."
                )
                embed = discord.Embed(
                    title=KEISH_PROC_TITLE,
                    description=blessing_proc_description(),
                    color=discord.Color.gold(),
                )
                keish_energy_file = None
                if KEISH_ENERGY_GIF_PATH.is_file():
                    keish_energy_file = discord.File(KEISH_ENERGY_GIF_PATH, filename="keish_energy.gif")
                    embed.set_image(url="attachment://keish_energy.gif")
                embed.set_footer(text=cd_msg)
                if keish_energy_file:
                    await ctx.reply(embed=embed, file=keish_energy_file, mention_author=False)
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
                steal_result = self._get_random_user_with_stealable_ducks(exclude_user_id=new_owner_id)
            
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
                    theft_text = (
                        f"\n⚠️ <@{prev_owner_id}> — your duck **{duck_row['name']}** was stolen!\n"
                        "🌀 Your cooldown has been refreshed. Use `!duck` within **5 minutes** to trigger revenge."
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

            # 4) Keish blessing window (from a prior proc)
            blessing_now = self.keish.active(new_owner_id)
            if blessing_now:
                rem_disp = self.keish.remaining_display(new_owner_id)
                cd_msg = f"✨ Keish's ENERGY active—**{rem_disp}s** left; **no `!duck` cooldown** until it ends."
            else:
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

            if blessing_now:
                # Blessing window: full duck card with Keish-themed titles.
                color = RARITY_EMBED_COLORS.get(rarity, discord.Color.dark_grey())
                title = KEISH_SADDLE_TITLE if shiny else KEISH_SNAG_TITLE
                catch_body = (
                    f"{flair}\n\n"
                    f"**Name:** {name}\n"
                    f"**Rarity:** {rarity}{' ✨' if shiny else ''}\n"
                    f"**Stats:** ⚔️ {atk}  🛡️ {dfs}  💨 {spd}\n"
                )
            else:
                color = RARITY_EMBED_COLORS.get(rarity, discord.Color.dark_grey())
                title = (
                    f"{'🌟✨ Shiny Duck Appeared! ✨🌟 ' if shiny else ''}"
                    f"Congrats on your new duck!{' SO SHINY!' if shiny else ''}"
                )
                catch_body = (
                    f"{flair}\n\n"
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
            if ctx.guild.get_member(int(user_id)) is not None
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
            title="🏆 Duck Leaderboard 🏆",
            description="\n".join(lines),
            color=discord.Color.gold()
        )
        embed.set_footer(text=f"Total ducks caught in the server: {total_ducks}")

        await ctx.reply(embed=embed)
    
    @commands.command(name="showcase")
    async def showcase_duck(self, ctx: commands.Context, *, duck_name: str):
        """Showcase any duck by name (shows owner, rarity, stats, age, and image)."""
        row = self._get_owned_duck_row_by_name(str(ctx.author.id), duck_name)
        if not row:
            cur = self.db.cursor()
            cur.execute(
                "SELECT * FROM ducks WHERE name = ? ORDER BY duck_id LIMIT 1",
                (duck_name,),
            )
            row = cur.fetchone()

        if not row:
            await ctx.reply(f"No duck named **{duck_name}** was found.")
            return

        # Owner info
        owner_id = row["owner_id"]
        if owner_id:
            member = ctx.guild.get_member(int(owner_id))
            owner_name = member.display_name if member else f"User {owner_id}"
        else:
            owner_name = "Unowned"

        color = RARITY_EMBED_COLORS.get(row["rarity"], discord.Color.dark_grey())

        shiny = bool(row["shiny"])

        age_str = _fmt_age_ago(row["timestamp"])

        # Embed
        embed = discord.Embed(
            title=f"{'✨ ' if shiny else ''}{row['name']}{' ✨' if shiny else ''}",
            description=(
                f"**Owner:** {owner_name}\n"
                f"**Rarity:** {row['rarity']}{' ✨' if shiny else ''}\n"
                f"**Stats:** ⚔️ {row['attack']}  🛡️ {row['defense']}  💨 {row['speed']}\n"
                f"**Caught:** {age_str}"
            ),
            color=color
        )
        embed.set_image(url=row["url"])
        embed.set_footer(text="Duck Showcase")

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

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        await self._handle_duck_role_reaction(payload, added=True)

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent):
        await self._handle_duck_role_reaction(payload, added=False)
