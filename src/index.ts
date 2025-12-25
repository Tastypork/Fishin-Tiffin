import { Client, GatewayIntentBits, REST, Routes } from "discord.js";
import "dotenv/config";

const client = new Client({
  intents: [GatewayIntentBits.Guilds],
});

client.once("ready", () => {
  console.log(`Logged in as ${client.user?.tag}`);
});

/**
 * Register a simple /ping command (guild-scoped for instant updates)
 */
async function registerCommands() {
  const commands = [
    {
      name: "ping",
      description: "Replies with Pong!",
    },
  ];

  const rest = new REST({ version: "10" }).setToken(process.env.DISCORD_TOKEN!);

  await rest.put(
    Routes.applicationCommands(
      process.env.APPLICATION_ID!,
    ),
    { body: commands }
  );

  console.log("Slash commands registered");
}

client.on("interactionCreate", async (interaction) => {
  if (!interaction.isChatInputCommand()) return;

  if (interaction.commandName === "ping") {
    await interaction.reply("Pong!");
  }
});

(async () => {
  await registerCommands();
  await client.login(process.env.DISCORD_TOKEN);
})();
