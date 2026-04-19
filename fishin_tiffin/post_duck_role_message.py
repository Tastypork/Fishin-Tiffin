from __future__ import annotations

import logging

import discord

from .bot import FishinTiffin
from .duck_manager import DUCK_ROLE_EMOJI, build_duck_onboarding_embed


async def post_duck_role_message(channel_id: int | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    intents = discord.Intents.default()
    intents.guilds = True
    intents.members = True
    intents.message_content = False

    bot = FishinTiffin(command_prefix="!", intents=intents, help_command=None)

    @bot.event
    async def on_ready() -> None:
        if bot.duck_role is None:
            print("Missing 'duck_role' in config.yml. Add duck_role: <role_id> and run again.")
            await bot.close()
            return

        target_channel_id = channel_id or bot.roles_channel or bot.ducks
        channel = bot.get_channel(target_channel_id)
        if not isinstance(channel, discord.TextChannel):
            try:
                channel = await bot.fetch_channel(target_channel_id)
            except discord.HTTPException as exc:
                print(f"Failed to fetch channel {target_channel_id}: {exc}")
                await bot.close()
                return
            if not isinstance(channel, discord.TextChannel):
                print(f"Channel {target_channel_id} is not a text channel.")
                await bot.close()
                return

        role = channel.guild.get_role(bot.duck_role)
        if role is None:
            print(f"Role {bot.duck_role} was not found in guild {channel.guild.id}.")
            await bot.close()
            return

        embed = build_duck_onboarding_embed(role_mention=role.mention)
        msg = await channel.send(embed=embed)
        await msg.add_reaction(DUCK_ROLE_EMOJI)
        print(f"Posted onboarding message in #{channel.name} ({channel.id}) with message ID {msg.id}.")
        await bot.close()

    await bot.start(bot.token)
