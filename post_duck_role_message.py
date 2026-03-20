from __future__ import annotations

import asyncio
import logging
import sys

import discord

from bot import FishinTiffin
from duck_manager import DUCK_ROLE_EMOJI, build_duck_onboarding_embed


async def post_duck_role_message(channel_id: int | None = None) -> None:
    intents = discord.Intents.default()
    intents.guilds = True
    intents.members = True
    intents.message_content = False

    bot = FishinTiffin(command_prefix="!", intents=intents, help_command=None)

    logging.basicConfig(
        level=getattr(logging, str(bot.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    @bot.event
    async def on_ready() -> None:
        role_id = bot.duck_role
        if role_id is None:
            print("Missing 'duck_role' in config.yml. Add duck_role: <role_id> and run again.")
            await bot.close()
            return

        target_channel_id = channel_id or bot.roles_channel or bot.ducks
        channel = bot.get_channel(target_channel_id)
        if not isinstance(channel, discord.TextChannel):
            try:
                fetched_channel = await bot.fetch_channel(target_channel_id)
            except discord.HTTPException as exc:
                print(f"Failed to fetch channel {target_channel_id}: {exc}")
                await bot.close()
                return
            if not isinstance(fetched_channel, discord.TextChannel):
                print(f"Channel {target_channel_id} is not a text channel.")
                await bot.close()
                return
            channel = fetched_channel

        role = channel.guild.get_role(role_id)
        if role is None:
            print(f"Role {role_id} was not found in guild {channel.guild.id}.")
            await bot.close()
            return

        embed = build_duck_onboarding_embed(role_mention=role.mention)
        msg = await channel.send(embed=embed)
        await msg.add_reaction(DUCK_ROLE_EMOJI)
        print(f"Posted onboarding message in #{channel.name} ({channel.id}) with message ID {msg.id}.")
        await bot.close()

    await bot.start(bot.token)


def _parse_args(argv: list[str]) -> int | None:
    if len(argv) not in (1, 2):
        raise ValueError("Usage: python post_duck_role_message.py [channel_id]")
    return int(argv[1]) if len(argv) == 2 else None


if __name__ == "__main__":
    try:
        parsed_channel_id = _parse_args(sys.argv)
    except ValueError as exc:
        print(exc)
        raise SystemExit(1) from exc

    asyncio.run(post_duck_role_message(parsed_channel_id))
