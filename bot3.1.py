#!/usr/bin/env python3
"""
Role Cycler Discord Bot

Implements:
- Slash commands under a configurable command starter (group), e.g. /rolecycler
- Public announcements for selections (scheduler and manual run)
- Non-repeating selections within a batch when there are enough users with role A
- Cycle logic so everyone with role A gets role B at least once per cycle; if z < y left, start a new cycle and fill
- JSON persistence (config, cycle state, stats, permissions)
- Track per-user total time holding role B
- Two languages: English (en) and Croatian (hr)
- Command permission system (admins OR allowed users/roles)
- Scheduler: presets (daily/weekly/monthly) and flexible every-N-days
- Removal of stale/duplicate commands on startup for the configured command group

Tested with: discord.py >= 2.3 (uses app_commands)
Python >= 3.10
"""

import asyncio
import json
import logging
import os
import random
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set

import discord
from discord import app_commands
from zoneinfo import ZoneInfo

# ------------- Configuration file paths -------------

DATA_PATH = os.environ.get("RC_DATA_FILE", "data.json")

# ------------- Logging -------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("rolecycler")

# ------------- Data models -------------

@dataclass
class ScheduleConfig:
    mode: str = "preset"          # "preset" or "every_days"
    preset: str = "weekly"        # "daily", "weekly", "monthly" (used if mode == "preset")
    every_days: int = 7           # used if mode == "every_days"
    hour: int = 12
    minute: int = 0

@dataclass
class GuildConfig:
    guild_id: int
    command_starter: str = "khan"
    role_a_id: int = 0
    role_b_id: int = 0
    picks_number: int = 2
    language: str = "en"  # "en" or "hr"
    announcement_channel_id: Optional[int] = None
    timezone: str = "Europe/Zagreb"
    purge_stale_commands_on_start: bool = True
    use_global_commands: bool = False  # if True, registers globally too
    schedule: ScheduleConfig = ScheduleConfig()

@dataclass
class UserStats:
    total_seconds_with_b: int = 0
    last_assigned_at: Optional[float] = None  # POSIX timestamp when role B was last assigned

@dataclass
class GuildState:
    # Users still waiting in the current cycle (user IDs)
    current_cycle_pool: List[int] = None

    # Permissions
    allowed_user_ids: Set[int] = None
    allowed_role_ids: Set[int] = None

    # Stats per user
    stats: Dict[int, UserStats] = None

    def __post_init__(self):
        if self.current_cycle_pool is None:
            self.current_cycle_pool = []
        if self.allowed_user_ids is None:
            self.allowed_user_ids = set()
        if self.allowed_role_ids is None:
            self.allowed_role_ids = set()
        if self.stats is None:
            self.stats = {}

# ------------- Localization -------------

MESSAGES = {
    "en": {
        "help_title": "📘 Role Cycler — Commands",
        "help_body": (
            "🔹 **/{} help** — 📖 Show this help.\n"
            "🔹 **/{} run** — 🎯 Randomly assign role B to the next Y users from role A and remove role B from previous holders (public). Admins or permitted users only.\n"
            "🔹 **/{} status** — 📊 Show current B holders and the planned order until the end of the cycle in groups of Y.\n"
            "🔹 **/{} picks-number show** — 🔢 Show current Y.\n"
            "🔹 **/{} picks-number set {num}** — ⚙️ Set Y. Admins or permitted users only.\n"
            "🔹 **/{} language show** — 🌐 Show current language.\n"
            "🔹 **/{} language set {en|hr}** — 🌐 Change language. Admins or permitted users only.\n"
            "🔹 **/{} permissions list** — 🔐 List permitted users/roles.\n"
            "🔹 **/{} permissions add_user|remove_user {user}** — 👤 Manage user permission. Admins or permitted users only.\n"
            "🔹 **/{} permissions add_role|remove_role {role}** — 📛 Manage role permission. Admins or permitted users only.\n"
            "🔹 **/{} schedule preset {daily|weekly|monthly} [hour] [minute]** — 🗓️ Use a preset schedule. Admins or permitted users only.\n"
            "🔹 **/{} schedule every-days {days} [hour] [minute]** — 🗓️ Flexible every-N-days schedule. Admins or permitted users only.\n"
            "🔹 **/{} schedule show** — 🗓️ Show current schedule.\n"
            "🔹 **/{} schedule stop** — 🛑 Stop the automatic scheduler."
        ),
        "run_public_header": "🎲 New selection for role B",
        "run_no_channel": "⚠️ Announcement channel not set; sending here.",
        "run_summary": "✅ Assigned role B: {mentions}\n❌ Removed role B: {removed_mentions}",
        "run_insufficient": "🚫 Not enough users with role A to fill Y without repeats; some users may repeat across cycles.",
        "status_title": "📊 Role B Status",
        "status_current": "👥 Currently holding B ({n}): {mentions}",
        "status_queue": "⏭️ Next in cycle (in groups of {y}):\n{lines}",
        "picks_show": "🎯 Current Y (picks per run): **{y}**",
        "picks_set": "🛠️ Picks per run set to **{y}**",
        "lang_show": "🌐 Current language: **{lang}**",
        "lang_set": "🌐 Language set to **{lang}**",
        "perm_list_header": "🔐 Permissions",
        "perm_list_users": "👤 Users: {users}",
        "perm_list_roles": "📛 Roles: {roles}",
        "perm_changed": "✅ Permissions updated.",
        "schedule_show": "🗓️ Schedule: **{desc}** (Time: {hh:02d}:{mm:02d} @ {tz})",
        "schedule_set": "🗓️ Schedule updated: **{desc}** at {hh:02d}:{mm:02d}",
        "schedule_stopped": "⏹️ Scheduler stopped.",
        "config_needed": "⚠️ This server isn't configured yet. Admins: set role A, role B, picks number, and schedule in data.json or via commands.",
        "not_permitted": "🚫 You don't have permission to use this command.",
        "debug_starting": "🔄 Starting up… syncing commands and scheduler.",
        "debug_synced": "🔄 Commands synced for guild {gid}.",
        "debug_purged": "🧹 Purged {n} stale commands for guild {gid}.",
        "debug_run": "🕹️ Manual run invoked by {user} in guild {gid}.",
        "debug_sched_run": "⏱️ Scheduled run executed in guild {gid}.",
        "debug_saved": "💾 State saved for guild {gid}.",
        "debug_error": "❗ Error: {err}",
        "debug_config": "📥 Loaded config for guild {gid}.",
    },
    "hr": {
        "help_title": "📘 Role Cycler — Naredbe",
        "help_body": (
            "🔹 **/{} help** — 📖 Prikaži pomoć.\n"
            "🔹 **/{} run** — 🎯 Nasumično dodijeli ulogu B sljedećim Y korisnicima s ulogom A i ukloni ulogu B prethodnim (javno). Samo administratori ili ovlašteni korisnici.\n"
            "🔹 **/{} status** — 📊 Prikaži tko trenutno ima B i redoslijed do kraja ciklusa u grupama od Y.\n"
            "🔹 **/{} picks-number show** — 🔢 Prikaži trenutačni Y.\n"
            "🔹 **/{} picks-number set {num}** — ⚙️ Postavi Y. Samo administratori ili ovlašteni korisnici.\n"
            "🔹 **/{} language show** — 🌐 Prikaži jezik.\n"
            "🔹 **/{} language set {en|hr}** — 🌐 Promijeni jezik. Samo administratori ili ovlašteni korisnici.\n"
            "🔹 **/{} permissions list** — 🔐 Prikaži ovlasti.\n"
            "🔹 **/{} permissions add_user|remove_user {user}** — 👤 Uredi ovlast korisnika. Samo administratori ili ovlašteni korisnici.\n"
            "🔹 **/{} permissions add_role|remove_role {role}** — 📛 Uredi ovlast uloge. Samo administratori ili ovlašteni korisnici.\n"
            "🔹 **/{} schedule preset {daily|weekly|monthly} [sat] [minuta]** — 🗓️ Zadani raspored. Samo administratori ili ovlašteni korisnici.\n"
            "🔹 **/{} schedule every-days {dana} [sat] [minuta]** — 🗓️ Fleksibilno, svakih N dana. Samo administratori ili ovlašteni korisnici.\n"
            "🔹 **/{} schedule show** — 🗓️ Prikaži raspored.\n"
            "🔹 **/{} schedule stop** — 🛑 Zaustavi automatsko pokretanje."
        ),
        "run_public_header": "🎲 Novi odabir za ulogu B",
        "run_no_channel": "⚠️ Kanal za objave nije postavljen; šaljem ovdje.",
        "run_summary": "✅ Uloga B dodijeljena: {mentions}\n❌ Uklonjeno s: {removed_mentions}",
        "run_insufficient": "🚫 Nema dovoljno korisnika s ulogom A za Y bez ponavljanja; neki se mogu ponoviti preko ciklusa.",
        "status_title": "📊 Status uloge B",
        "status_current": "👥 Trenutno imaju B ({n}): {mentions}",
        "status_queue": "⏭️ Sljedeći u ciklusu (u grupama od {y}):\n{lines}",
        "picks_show": "🎯 Trenutni Y (broj dodjela po pokretanju): **{y}**",
        "picks_set": "🛠️ Postavljeno Y na **{y}**",
        "lang_show": "🌐 Trenutni jezik: **{lang}**",
        "lang_set": "🌐 Jezik postavljen na **{lang}**",
        "perm_list_header": "🔐 Ovlasti",
        "perm_list_users": "👤 Korisnici: {users}",
        "perm_list_roles": "📛 Uloge: {roles}",
        "perm_changed": "✅ Ovlasti ažurirane.",
        "schedule_show": "🗓️ Raspored: **{desc}** (Vrijeme: {hh:02d}:{mm:02d} @ {tz})",
        "schedule_set": "🗓️ Raspored ažuriran: **{desc}** u {hh:02d}:{mm:02d}",
        "schedule_stopped": "⏹️ Raspored zaustavljen.",
        "config_needed": "⚠️ Ovaj server još nije konfiguriran. Admini: postavite uloge A i B, Y i raspored u data.json ili kroz naredbe.",
        "not_permitted": "🚫 Nemaš ovlasti za ovu naredbu.",
        "debug_starting": "🔄 Pokretanje… sinkroniziram naredbe i raspored.",
        "debug_synced": "🔄 Naredbe sinkronizirane za server {gid}.",
        "debug_purged": "🧹 Obrisano {n} zastarjelih naredbi za server {gid}.",
        "debug_run": "🕹️ Ručna naredba run pokrenuta od {user} na serveru {gid}.",
        "debug_sched_run": "⏱️ Zakazano pokretanje izvršeno na serveru {gid}.",
        "debug_saved": "💾 Stanje spremljeno za server {gid}.",
        "debug_error": "❗ Greška: {err}",
        "debug_config": "📥 Učitana konfiguracija za server {gid}.",
    },
}

# ------------- Persistence -------------

class Store:
    def __init__(self, path: str):
        self.path = path
        self._lock = asyncio.Lock()
        self.data = {
            "guilds": {}  # guild_id -> {"config": GuildConfig, "state": GuildState}
        }

    def _encode(self):
        # Convert dataclasses to plain dicts
        enc = {"guilds": {}}
        for gid, payload in self.data["guilds"].items():
            cfg: GuildConfig = payload["config"]
            st: GuildState = payload["state"]
            enc["guilds"][str(gid)] = {
                "config": {
                    **asdict(cfg),
                    "schedule": asdict(cfg.schedule),
                },
                "state": {
                    "current_cycle_pool": st.current_cycle_pool,
                    "allowed_user_ids": list(st.allowed_user_ids),
                    "allowed_role_ids": list(st.allowed_role_ids),
                    "stats": {str(uid): asdict(st.stats.get(uid, UserStats())) for uid in st.stats},
                },
            }
        return enc

    def _decode(self, raw):
        out = {"guilds": {}}
        for gid_str, payload in raw.get("guilds", {}).items():
            gid = int(gid_str)
            cfg_raw = payload.get("config", {})
            sched_raw = cfg_raw.get("schedule", {})
            cfg = GuildConfig(
                guild_id=gid,
                command_starter=cfg_raw.get("command_starter", "rolecycler"),
                role_a_id=int(cfg_raw.get("role_a_id", 0)),
                role_b_id=int(cfg_raw.get("role_b_id", 0)),
                picks_number=int(cfg_raw.get("picks_number", 2)),
                language=cfg_raw.get("language", "en"),
                announcement_channel_id=cfg_raw.get("announcement_channel_id"),
                timezone=cfg_raw.get("timezone", "Europe/Zagreb"),
                purge_stale_commands_on_start=cfg_raw.get("purge_stale_commands_on_start", True),
                use_global_commands=cfg_raw.get("use_global_commands", False),
                schedule=ScheduleConfig(
                    mode=sched_raw.get("mode", "preset"),
                    preset=sched_raw.get("preset", "weekly"),
                    every_days=int(sched_raw.get("every_days", 7)),
                    hour=int(sched_raw.get("hour", 12)),
                    minute=int(sched_raw.get("minute", 0)),
                ),
            )
            st_raw = payload.get("state", {})
            stats = {}
            for uid_str, s in st_raw.get("stats", {}).items():
                stats[int(uid_str)] = UserStats(
                    total_seconds_with_b=int(s.get("total_seconds_with_b", 0)),
                    last_assigned_at=s.get("last_assigned_at", None),
                )
            st = GuildState(
                current_cycle_pool=list(st_raw.get("current_cycle_pool", [])),
                allowed_user_ids=set(st_raw.get("allowed_user_ids", [])),
                allowed_role_ids=set(st_raw.get("allowed_role_ids", [])),
                stats=stats,
            )
            out["guilds"][gid] = {"config": cfg, "state": st}
        self.data = out

    async def load(self):
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            self._decode(raw)
        else:
            # Initialize empty file
            await self.save()

    async def save(self):
        async with self._lock:
            tmp = self._encode()
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(tmp, f, indent=2, ensure_ascii=False)

# ------------- Utility -------------

def human_lang(cfg: GuildConfig):
    return "Hrvatski" if cfg.language == "hr" else "English"

def msg(cfg: GuildConfig, key: str, **kwargs) -> str:
    return MESSAGES.get(cfg.language, MESSAGES["en"]).get(key, key).format(**kwargs)

def is_admin_or_permitted(interaction: discord.Interaction, cfg: GuildConfig, st: GuildState) -> bool:
    # Admin check
    if interaction.user.guild_permissions.administrator:
        return True
    # Allowed users
    if interaction.user.id in st.allowed_user_ids:
        return True
    # Allowed roles
    user_role_ids = {r.id for r in getattr(interaction.user, "roles", [])}
    if st.allowed_role_ids & user_role_ids:
        return True
    return False

def tz_now(cfg: GuildConfig) -> datetime:
    return datetime.now(ZoneInfo(cfg.timezone))

def compute_next_run(cfg: GuildConfig, now: Optional[datetime] = None) -> datetime:
    if now is None:
        now = tz_now(cfg)
    hour = cfg.schedule.hour
    minute = cfg.schedule.minute
    if cfg.schedule.mode == "preset":
        if cfg.schedule.preset == "daily":
            target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)
            return target
        elif cfg.schedule.preset == "weekly":
            # Next occurrence same weekday/time next week if passed
            target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if target <= now:
                target += timedelta(weeks=1)
            return target
        elif cfg.schedule.preset == "monthly":
            # naive: add ~30 days (good enough for our purpose)
            target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=30)
            return target
        else:
            # fallback daily
            target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)
            return target
    else:
        # every_days
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=max(1, cfg.schedule.every_days))
        return target

def chunked(seq, size):
    return [seq[i:i+size] for i in range(0, len(seq), size)]

# ------------- Bot -------------

intents = discord.Intents.default()
intents.members = True  # needed to fetch guild members to check roles
intents.guilds = True

bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)

store = Store(DATA_PATH)

scheduler_tasks: Dict[int, asyncio.Task] = {}  # guild_id -> task

async def ensure_guild_entry(guild: discord.Guild):
    gid = guild.id
    if gid not in store.data["guilds"]:
        cfg = GuildConfig(guild_id=gid)
        st = GuildState()
        store.data["guilds"][gid] = {"config": cfg, "state": st}
        await store.save()
        log.info(msg(cfg, "debug_config", gid=gid))

async def purge_stale_commands_for_guild(guild: discord.Guild, cfg: GuildConfig):
    # Delete existing remote commands with our command_starter to avoid duplicates
    try:
        removed = 0
        cmds = await tree.fetch_commands(guild=guild)
        for c in cmds:
            if c.name == cfg.command_starter:
                await tree.delete_command(c, guild=guild)
                removed += 1
        log.info(msg(cfg, "debug_purged", gid=guild.id, n=removed))
    except Exception as e:
        log.warning(f"Could not purge stale commands for guild {guild.id}: {e}")

async def sync_commands_for_guild(guild: discord.Guild, cfg: GuildConfig):
    try:
        await tree.sync(guild=guild)
        log.info(msg(cfg, "debug_synced", gid=guild.id))
    except Exception as e:
        log.error(f"Sync failed for guild {guild.id}: {e}")

async def scheduled_runner(guild: discord.Guild):
    await ensure_guild_entry(guild)
    payload = store.data["guilds"][guild.id]
    cfg: GuildConfig = payload["config"]
    st: GuildState = payload["state"]

    while True:
        now = tz_now(cfg)
        next_time = compute_next_run(cfg, now)
        delay = (next_time - now).total_seconds()
        await asyncio.sleep(delay)
        try:
            await perform_run(guild, cfg, st, scheduled=True)
        except Exception as e:
            log.error(msg(cfg, "debug_error", err=e))

async def perform_run(guild: discord.Guild, cfg: GuildConfig, st: GuildState, scheduled: bool):
    """
    Core logic to remove B from previous holders, update stats, and assign B to new picks.
    Announce publicly in the configured channel, or fallback to system channel if set, else log warning.
    """
    # Fetch roles
    role_a = guild.get_role(cfg.role_a_id)
    role_b = guild.get_role(cfg.role_b_id)
    if not role_a or not role_b:
        log.warning(f"Guild {guild.id}: roles not configured correctly.")
        return

    # Members with role A (excluding bots)
    members_with_a = [m for m in guild.members if role_a in m.roles and not m.bot]

    # Remove B from everyone who currently has it, updating time stats
    prev_b_holders = [m for m in guild.members if role_b in m.roles]
    now_ts = datetime.now().timestamp()
    removed_mentions = []
    for m in prev_b_holders:
        # Update stats
        us = st.stats.get(m.id, UserStats())
        if us.last_assigned_at is not None:
            us.total_seconds_with_b += int(now_ts - us.last_assigned_at)
            us.last_assigned_at = None
        st.stats[m.id] = us
        # Remove role
        try:
            await m.remove_roles(role_b, reason="Role Cycler — cycle advance")
            removed_mentions.append(m.mention)
        except Exception as e:
            log.warning(f"Failed to remove B from {m} in guild {guild.id}: {e}")

    # Selection logic (no repeats within batch when enough users)
    y = max(1, int(cfg.picks_number))
    pool = list(st.current_cycle_pool)

    # Initialize pool if empty or contains users not in role A anymore
    pool = [uid for uid in pool if any(m.id == uid for m in members_with_a)]
    if not pool:
        pool = [m.id for m in members_with_a]
        random.shuffle(pool)

    picks: List[int] = []

    # Take from current pool first
    take = min(y, len(pool))
    picks.extend(pool[:take])
    pool = pool[take:]

    # If we still need more, start a new cycle and continue (avoid duplicates within the same batch if possible)
    need = y - len(picks)
    if need > 0:
        new_cycle = [m.id for m in members_with_a]
        random.shuffle(new_cycle)
        # avoid duplicates within batch
        new_cycle = [uid for uid in new_cycle if uid not in picks]
        if len(new_cycle) >= need:
            picks.extend(new_cycle[:need])
            pool = new_cycle[need:]  # remaining for the rest of the new cycle
        else:
            # Not enough unique users overall to fill Y; allow repeats across cycles but not within this batch
            picks.extend(new_cycle)
            # pool becomes empty; next run will start fresh
            pool = []

    # Update remaining pool
    st.current_cycle_pool = pool

    # Assign role B to picks
    assigned_mentions = []
    for uid in picks:
        member = guild.get_member(uid)
        if member is None:
            continue
        # Assign role B if they don't already have it
        try:
            await member.add_roles(role_b, reason="Role Cycler — selected")
            assigned_mentions.append(member.mention)
            us = st.stats.get(member.id, UserStats())
            us.last_assigned_at = now_ts
            st.stats[member.id] = us
        except Exception as e:
            log.warning(f"Failed to add B to {member} in guild {guild.id}: {e}")

    # Announce publicly
    channel = None
    if cfg.announcement_channel_id:
        channel = guild.get_channel(cfg.announcement_channel_id)
    if channel is None:
        # Try system channel
        channel = guild.system_channel
    # If still none (no system channel), we cannot post — just log
    text_header = msg(cfg, "run_public_header")
    text_summary = msg(cfg, "run_summary",
                       mentions=", ".join(assigned_mentions) if assigned_mentions else "—",
                       removed_mentions=", ".join(removed_mentions) if removed_mentions else "—")
    if channel is not None:
        await channel.send(f"**{text_header}**\n{text_summary}")
    else:
        log.warning("No announcement channel available to send message.")

    # Note about insufficient unique users
    if len(members_with_a) < y:
        warn = msg(cfg, "run_insufficient")
        if channel:
            await channel.send(warn)
        else:
            log.info(warn)

    await store.save()
    log.info(msg(cfg, "debug_saved", gid=guild.id))

# ------------- Events -------------

@bot.event
async def on_ready():
    await store.load()
    log.info("Bot connected as %s", bot.user)
    # Ensure entries for all guilds the bot is in
    for guild in bot.guilds:
        await ensure_guild_entry(guild)

    # Purge and sync commands, start schedulers
    for guild in bot.guilds:
        payload = store.data["guilds"][guild.id]
        cfg: GuildConfig = payload["config"]
        st: GuildState = payload["state"]
        log.info(msg(cfg, "debug_starting"))
        if cfg.purge_stale_commands_on_start:
            await purge_stale_commands_for_guild(guild, cfg)
        await sync_commands_for_guild(guild, cfg)
        # Start scheduler task per guild
        if guild.id in scheduler_tasks:
            scheduler_tasks[guild.id].cancel()
        scheduler_tasks[guild.id] = asyncio.create_task(scheduled_runner(guild))

# ------------- Command Group -------------

def get_group_name_for_guild(guild_id: int) -> str:
    payload = store.data["guilds"].get(guild_id)
    if not payload:
        return "rolecycler"
    return payload["config"].command_starter or "rolecycler"

def group_for_guild(guild_id: int):
    # Return or create an app_commands.Group with dynamic name per guild
    name = get_group_name_for_guild(guild_id)
    return app_commands.Group(name=name, description="Role Cycler commands")

# We'll register commands dynamically per guild in on_guild_available

@bot.event
async def on_guild_available(guild: discord.Guild):
    # Clear any existing commands for this guild in the tree for dynamic name changes
    # Then add the group commands
    await ensure_guild_entry(guild)
    payload = store.data["guilds"][guild.id]
    cfg: GuildConfig = payload["config"]
    st: GuildState = payload["state"]

    # Remove previous group (local) definitions for this guild if they exist
    for cmd in list(tree.get_commands()):
        if isinstance(cmd, app_commands.Group) and cmd.name == cfg.command_starter:
            tree.remove_command(cmd.name, guild=guild)

    # Build a fresh group
    grp = group_for_guild(guild.id)

    @grp.command(name="help", description="Show help")
    async def help_cmd(interaction: discord.Interaction):
        await interaction.response.send_message(
            f"__{msg(cfg, 'help_title')}__\n" +
            msg(cfg, "help_body", *(cfg.command_starter for _ in range(13))),
            ephemeral=False
        )

    @grp.command(name="status", description="Show current and upcoming selections")
    async def status_cmd(interaction: discord.Interaction):
        role_b = interaction.guild.get_role(cfg.role_b_id)
        current = [m.mention for m in interaction.guild.members if role_b in m.roles] if role_b else []
        y = max(1, int(cfg.picks_number))
        pool = list(st.current_cycle_pool)
        # Build preview in groups of y
        groups = chunked(pool, y)
        lines = []
        for i, g in enumerate(groups, start=1):
            mentions = []
            for uid in g:
                m = interaction.guild.get_member(uid)
                if m:
                    mentions.append(m.mention)
            if mentions:
                lines.append(f"**Group {i}:** " + ", ".join(mentions))
        body = f"**{msg(cfg, 'status_title')}**\n" + msg(cfg, "status_current", n=len(current), mentions=", ".join(current) if current else "—")
        if lines:
            body += "\n" + msg(cfg, "status_queue", y=y, lines="\n".join(lines))
        await interaction.response.send_message(body, ephemeral=False)

    @grp.command(name="run", description="Run a manual cycle now")
    async def run_cmd(interaction: discord.Interaction):
        payload = store.data["guilds"][interaction.guild_id]
        cfg2: GuildConfig = payload["config"]
        st2: GuildState = payload["state"]
        if not is_admin_or_permitted(interaction, cfg2, st2):
            await interaction.response.send_message(msg(cfg2, "not_permitted"), ephemeral=True)
            return
        log.info(msg(cfg2, "debug_run", user=str(interaction.user), gid=interaction.guild_id))
        await interaction.response.send_message("⏳ Running selection…", ephemeral=True)
        await perform_run(interaction.guild, cfg2, st2, scheduled=False)

    picks_grp = app_commands.Group(name="picks-number", description="Show or set picks per run")

    @picks_grp.command(name="show", description="Show Y")
    async def picks_show(interaction: discord.Interaction):
        await interaction.response.send_message(msg(cfg, "picks_show", y=cfg.picks_number), ephemeral=False)

    @picks_grp.command(name="set", description="Set Y")
    @app_commands.describe(num="Number of users to assign role B per run")
    async def picks_set(interaction: discord.Interaction, num: app_commands.Range[int, 1, 100]):
        if not is_admin_or_permitted(interaction, cfg, st):
            await interaction.response.send_message(msg(cfg, "not_permitted"), ephemeral=True)
            return
        cfg.picks_number = int(num)
        await store.save()
        await interaction.response.send_message(msg(cfg, "picks_set", y=cfg.picks_number), ephemeral=False)

    grp.add_command(picks_grp)

    lang_grp = app_commands.Group(name="language", description="Show or set language")

    @lang_grp.command(name="show", description="Show language")
    async def lang_show(interaction: discord.Interaction):
        await interaction.response.send_message(msg(cfg, "lang_show", lang=human_lang(cfg)), ephemeral=False)

    @lang_grp.command(name="set", description="Set language")
    @app_commands.describe(language="en or hr")
    async def lang_set(interaction: discord.Interaction, language: str):
        if not is_admin_or_permitted(interaction, cfg, st):
            await interaction.response.send_message(msg(cfg, "not_permitted"), ephemeral=True)
            return
        language = language.lower()
        if language not in ("en", "hr"):
            await interaction.response.send_message("Use: en or hr / Koristi: en ili hr", ephemeral=True)
            return
        cfg.language = language
        await store.save()
        await interaction.response.send_message(msg(cfg, "lang_set", lang=human_lang(cfg)), ephemeral=False)

    perm_grp = app_commands.Group(name="permissions", description="Manage permissions")

    @perm_grp.command(name="list", description="List permissions")
    async def perm_list(interaction: discord.Interaction):
        users = ", ".join(f"<@{uid}>" for uid in st.allowed_user_ids) or "—"
        roles = ", ".join(f"<@&{rid}>" for rid in st.allowed_role_ids) or "—"
        await interaction.response.send_message(
            f"**{msg(cfg, 'perm_list_header')}**\n" +
            msg(cfg, "perm_list_users", users=users) + "\n" +
            msg(cfg, "perm_list_roles", roles=roles), ephemeral=False)

    @perm_grp.command(name="add_user", description="Allow a user")
    async def perm_add_user(interaction: discord.Interaction, user: discord.User):
        if not is_admin_or_permitted(interaction, cfg, st):
            await interaction.response.send_message(msg(cfg, "not_permitted"), ephemeral=True)
            return
        st.allowed_user_ids.add(user.id)
        await store.save()
        await interaction.response.send_message(msg(cfg, "perm_changed"), ephemeral=False)

    @perm_grp.command(name="remove_user", description="Remove a user permission")
    async def perm_remove_user(interaction: discord.Interaction, user: discord.User):
        if not is_admin_or_permitted(interaction, cfg, st):
            await interaction.response.send_message(msg(cfg, "not_permitted"), ephemeral=True)
            return
        st.allowed_user_ids.discard(user.id)
        await store.save()
        await interaction.response.send_message(msg(cfg, "perm_changed"), ephemeral=False)

    @perm_grp.command(name="add_role", description="Allow a role")
    async def perm_add_role(interaction: discord.Interaction, role: discord.Role):
        if not is_admin_or_permitted(interaction, cfg, st):
            await interaction.response.send_message(msg(cfg, "not_permitted"), ephemeral=True)
            return
        st.allowed_role_ids.add(role.id)
        await store.save()
        await interaction.response.send_message(msg(cfg, "perm_changed"), ephemeral=False)

    @perm_grp.command(name="remove_role", description="Remove a role permission")
    async def perm_remove_role(interaction: discord.Interaction, role: discord.Role):
        if not is_admin_or_permitted(interaction, cfg, st):
            await interaction.response.send_message(msg(cfg, "not_permitted"), ephemeral=True)
            return
        st.allowed_role_ids.discard(role.id)
        await store.save()
        await interaction.response.send_message(msg(cfg, "perm_changed"), ephemeral=False)

    grp.add_command(perm_grp)

    sched_grp = app_commands.Group(name="schedule", description="Show or set scheduler")

    @sched_grp.command(name="show", description="Show schedule")
    async def sched_show(interaction: discord.Interaction):
        desc = cfg.schedule.preset if cfg.schedule.mode == "preset" else f"every {cfg.schedule.every_days} days"
        await interaction.response.send_message(
            msg(cfg, "schedule_show", desc=desc, hh=cfg.schedule.hour, mm=cfg.schedule.minute, tz=cfg.timezone),
            ephemeral=False)

    @sched_grp.command(name="preset", description="Set preset schedule")
    async def sched_preset(interaction: discord.Interaction, preset: app_commands.Choice[str], hour: Optional[int] = None, minute: Optional[int] = None):
        if not is_admin_or_permitted(interaction, cfg, st):
            await interaction.response.send_message(msg(cfg, "not_permitted"), ephemeral=True)
            return
        cfg.schedule.mode = "preset"
        cfg.schedule.preset = preset.value
        if hour is not None: cfg.schedule.hour = max(0, min(23, hour))
        if minute is not None: cfg.schedule.minute = max(0, min(59, minute))
        await store.save()
        await interaction.response.send_message(
            msg(cfg, "schedule_set", desc=cfg.schedule.preset, hh=cfg.schedule.hour, mm=cfg.schedule.minute),
            ephemeral=False)

    sched_preset.autocomplete("preset")(lambda i, c: [
        app_commands.Choice(name="daily", value="daily"),
        app_commands.Choice(name="weekly", value="weekly"),
        app_commands.Choice(name="monthly", value="monthly"),
    ])

    @sched_grp.command(name="every-days", description="Set flexible every-N-days schedule")
    async def sched_every_days(interaction: discord.Interaction, days: app_commands.Range[int, 1, 365], hour: Optional[int] = None, minute: Optional[int] = None):
        if not is_admin_or_permitted(interaction, cfg, st):
            await interaction.response.send_message(msg(cfg, "not_permitted"), ephemeral=True)
            return
        cfg.schedule.mode = "every_days"
        cfg.schedule.every_days = int(days)
        if hour is not None: cfg.schedule.hour = max(0, min(23, hour))
        if minute is not None: cfg.schedule.minute = max(0, min(59, minute))
        await store.save()
        await interaction.response.send_message(
            msg(cfg, "schedule_set", desc=f"every {cfg.schedule.every_days} days", hh=cfg.schedule.hour, mm=cfg.schedule.minute),
            ephemeral=False)

    @sched_grp.command(name="stop", description="Stop scheduler")
    async def sched_stop(interaction: discord.Interaction):
        if not is_admin_or_permitted(interaction, cfg, st):
            await interaction.response.send_message(msg(cfg, "not_permitted"), ephemeral=True)
            return
        task = scheduler_tasks.get(interaction.guild_id)
        if task:
            task.cancel()
            scheduler_tasks.pop(interaction.guild_id, None)
        await interaction.response.send_message(msg(cfg, "schedule_stopped"), ephemeral=False)

    grp.add_command(sched_grp)

    # Register the group with the tree for this guild
    tree.add_command(grp, guild=guild)
    if cfg.use_global_commands:
        tree.add_command(grp)  # also global (optional)

    # Sync after adding new group
    await sync_commands_for_guild(guild, cfg)

# ------------- Entrypoint -------------

def main():
    token = os.environ.get("DISCORD_BOT_TOKEN") or ""
    if not token:
        print("Please set DISCORD_BOT_TOKEN environment variable.")
        raise SystemExit(1)

    # Preload store to ensure file exists
    loop = asyncio.get_event_loop()
    loop.run_until_complete(store.load())

    bot.run(token)

if __name__ == "__main__":
    main()
