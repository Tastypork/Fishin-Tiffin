# FishinTiffin

Discord bot front-end for the duck game. **All game rules and persistence live in [Duck-Game-backend](../Duck-Game-backend)** - this package only maps API responses to Discord messages (embeds, attachments, reaction roles).

## Capabilities

- `!duck` - catch flow (cooldown, Keish, Zay, theft, revenge) via `POST /v1/duck/catch`
- `!battle` - winner-takes-loser's-duck battle via `POST /v1/duck/battle` (UTC hourly limit; wins chain)
- `!ducks [@member]` - dashboard link + count from `GET /me`
- `!leaderboard` - `GET /leaderboard`
- `!give @member <duck_name>` - `POST /v1/ducks/give`
- `!help` - in-channel command reference
- Reaction-role listeners on `🦆` for onboarding messages created by the bot
- Typo helper (`!duk` etc.) still triggers a catch through the API

## Project layout

- `main.py` - entry point (`fishin_tiffin.bot.run_bot`)
- `fishin_tiffin/bot.py` - Discord client, config loader, shared `aiohttp` session
- `fishin_tiffin/duck_manager.py` - commands, API client, embed dispatch, role reactions, announcement relay
- `fishin_tiffin/post_duck_role_message.py` - one-shot onboarding message poster
- `assets/dap.gif` - typo helper reaction
- Game art is served by **Duck-Game-backend** at `/static/game/`; the bot uses `image_url` from API responses (and uploads the file directly when the API host is private).

## Setup

1. Run **Duck-Game-backend** (see that repo's README), e.g. `https://api.duckgame.app` or `http://127.0.0.1:19999` locally.
2. Python venv and `pip install -r requirements.txt`
3. Copy `config.example.yml` to `config.yml` and fill in:
   - `token`, `ducks_channel`
   - `duck_game_api_url` - base URL of the API (default `https://api.duckgame.app`)
   - optional: `server`, `roles_channel`, `duck_role`, `duck_dashboard_base_url`, `leaderboard_season`, `duck_game_api_shared_secret`
4. `python main.py`

## Post the duck role message once

- Set `duck_role` in `config.yml`
- `python post_duck_role_message.py [channel_id]`
- Listeners in `duck_manager.py` add/remove the role when users react with `🦆` on bot messages in `roles_channel` (or the fallback channel).

## Web-origin announcements

When `duck_game_api_shared_secret` is configured, the bot polls `GET /v1/bot/pending-announcements` every 2s and relays any queued catch embeds (e.g. catches initiated from the web UI) into the channel the backend specifies, then ACKs them via `POST /v1/bot/announcements/ack`. Leave the secret unset to disable the poller.
