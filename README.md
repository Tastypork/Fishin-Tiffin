# FishinTiffin

Standalone Discord bot containing all duck gameplay capabilities extracted from YukiBot.

## Included Duck Capabilities

- `!duck` catch command with:
  - rarity roll (`Common`, `Uncommon`, `Rare`, `Legendary`, `Mythic`)
  - weighted stat rolls (attack, defense, speed)
  - shiny rolls
  - per-user randomized cooldown
  - 35% theft mechanic (steal random duck from another user)
- `!ducks [@member]` personal duck dashboard link generation
- `!leaderboard` top collectors leaderboard
- `!give @member <duck_name>` duck transfer between users
- reaction-role listeners on `🦆` for onboarding messages created by the bot
- SQLite-backed storage for ducks/users with auto migration for legacy schema
- Name pools for common and legendary tiers

## Project Layout

- `main.py` - entry point (runs `fishin_tiffin.bot`)
- `fishin_tiffin/` - application package (`bot.py`, `duck_manager.py` cog, energy helpers, HTML generator, paths)
- `post_duck_role_message.py` - thin wrapper → `python -m fishin_tiffin.post_duck_role_message` also works
- `duck_data/` - SQLite + generated HTML + name pools (repo root)
- `assets/` - GIF/PNG assets (repo root; paths resolved via `fishin_tiffin/paths.py`)
- `config.yml` - local runtime config (token + optional server + ducks channel + optional roles/duck role)

## Setup

1. Create a new virtual environment.
2. Install dependencies:
   - `pip install -r requirements.txt`
3. Edit `config.yml`:
   - set `token`
   - optionally set `server` to your Discord server (guild) ID
   - set `ducks_channel` to your Discord ducks channel ID
   - optionally set `roles_channel` to your roles channel ID
   - optionally set `duck_role` to your duck role ID (used by onboarding script)
4. Run:
   - `python main.py`

## Post The Duck Role Message Once

Use this one-time helper to manually post the onboarding message:

- set `duck_role` in `config.yml` first
- `python post_duck_role_message.py [channel_id]`
- If `channel_id` is omitted, it posts in `roles_channel` when set, otherwise `ducks_channel`.
- The message includes a short game explainer and auto-adds the `🦆` reaction.

The bot listeners in `fishin_tiffin/duck_manager.py` will then add/remove the role when users add/remove that reaction.
If `roles_channel` is set, only reactions in that channel are processed.

## Notes

- The bot requires two config keys: `token` and `ducks_channel`.
- `server` is optional and, when set, restricts commands to that guild ID.
- `roles_channel` is optional; when set, duck reaction-role handling is limited to that channel.
- `duck_role` is optional for normal gameplay, but required for `post_duck_role_message.py`.
- External endpoints are preserved from YukiBot in code defaults:
  - duck API: `https://duck.jocal.dev/duck`
  - dashboard base URL: `https://duck.jocal.dev/user`
- Data files are local to this repo under `duck_data`.

