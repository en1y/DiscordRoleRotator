# -----------------------------
# Role Cycler Discord Bot (slash-commands)
# -----------------------------
# Changes in this version:
# 1) On launch, clears guild commands and re-syncs them for every guild.
# 2) New /commands_starter stats command with CSV attachment of all holders' durations.
# 3) All /schedule commands are usable by everyone (no admin gate).
# 4) Default schedule: weekly on Friday at 00:00 (enabled).
# 5) /schedule show now also lists upcoming moderators (role B) to end of current queue.
# 6) Replies use emojis for a friendlier tone.
# -----------------------------

import os
import io
import csv
import json
import asyncio
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import random

import discord
from discord import app_commands
from discord.ext import commands

# ========== USER-TUNABLE DEFAULTS ==========
DEFAULT_PICKS_Y = 3
DEFAULT_SCHEDULE = {
    "enabled": True,              # Default enabled
    "type": "weekly",             # weekly schedule
    "n_days": 3,                  # kept for completeness (unused for weekly)
    "weekday": 4,                 # Friday (Mon=0..Sun=6)
    "dom": 1,                     # kept for completeness (unused for weekly)
    "time": "00:00"               # midnight
}
ROLE_A_NAME = "a"
ROLE_B_NAME = "b"
COMMAND_GROUP_NAME = "khan"
DEFAULT_LANGUAGE = "en"
DEFAULT_TZ = "Europe/Zagreb"

# ========== FILES / ENV ==========
DATA_FILE = Path("data.json")
LOG_FILE = Path("bot.log")
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")

# ========== LOGGING ==========
logger = logging.getLogger("role_cycler")
logger.setLevel(logging.DEBUG)
_fmt = logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")
sh = logging.StreamHandler()
sh.setFormatter(_fmt)
logger.addHandler(sh)
fh = RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
fh.setFormatter(_fmt)
logger.addHandler(fh)

# ========== I18N (with emojis) ==========
I18N = {
    "en": {
        "help_title": "🧭 Role Cycler — Commands",
        "help_body": (
            "• **/{cg} help** — Show this help.\n"
            "• **/{cg} run** — ▶️ Run one rotation now (pick next {y} users). *Admins & permitted only*.\n"
            "• **/{cg} status** — 📌 Show current holders and upcoming groups of {y}.\n"
            "• **/{cg} picks-number show** — 🔢 Show the number of picks (y).\n"
            "• **/{cg} picks-number set** — 🔧 Set the number of picks (y). *Admins & permitted only*.\n"
            "• **/{cg} language show** — 🌐 Show selected language.\n"
            "• **/{cg} language set** — 🗣️ Change language. *Admins & permitted only*.\n"
            "• **/{cg} permissions add_user/remove_user/add_role/remove_role** — 🛂 Manage who may run restricted commands.\n"
            "• **/{cg} permissions list** — 📜 List permitted users and roles.\n"
            "• **/{cg} schedule set** — ⏰ Configure automatic rotation (daily/weekly/monthly/every_n_days).\n"
            "• **/{cg} schedule show** — 📅 Show current schedule **and** upcoming moderators.\n"
            "• **/{cg} schedule stop** — 🛑 Disable automatic rotation.\n"
            "• **/{cg} stats** — 📊 Show total time each moderator held **{b}** (CSV attached)."
        ),
        "not_admin_or_permitted": "⚠️ You must be an administrator or permitted by this bot to use that command.",
        "missing_roles": "⚠️ Configured roles not found. Check role names **{a}** / **{b}**.",
        "run_start": "🔁 Starting rotation…",
        "run_done": "✅ Assigned **{b}** to: {mentions}",
        "run_none": "ℹ️ No eligible users with role **{a}** were found.",
        "status_title": "📌 Rotation Status",
        "status_current": "✅ **Currently holding {b} ({n}):** {mentions}",
        "status_none_current": "✅ **Currently holding {b}:** *(none)*",
        "status_upcoming": "🔮 **Upcoming (groups of {y}):**\n{blocks}",
        "picks_show": "🔢 Current picks (y) = **{y}**",
        "picks_set": "🔧 Updated picks (y) to **{y}**.",
        "lang_show": "🌐 Current language: **{lang}**",
        "lang_set": "🌐 Language updated to **{lang}**.",
        "perm_list": "📜 **Permitted Users:** {users}\n**Permitted Roles:** {roles}",
        "perm_added_user": "🛂 Added user permission: {mention}",
        "perm_removed_user": "🧹 Removed user permission: {mention}",
        "perm_added_role": "🛂 Added role permission: **{name}**",
        "perm_removed_role": "🧹 Removed role permission: **{name}**",
        "schedule_show_disabled": "🛑 Schedule is **disabled**.",
        "schedule_show_enabled": "📅 Schedule: **{desc}** — next run **{when}**.",
        "schedule_set": "✅ Schedule updated: **{desc}** — next run **{when}**.",
        "schedule_stopped": "🛑 Automatic schedule **disabled**.",
        "error": "❌ Error: {msg}",
        "startup_synced": "✅ Commands cleared & re-synced.",
        "startup_ready": "🤖 Bot is ready.",
        "stats_saved": "💾 Stats saved.",
        "stats_title": "📊 Stats for role **{b}**",
        "stats_none": "🙈 No stats yet — nobody has held **{b}**.",
        "stats_csv_note": "📎 Full stats CSV attached.",
        "schedule_upcoming_mods": "🧑‍⚖️ Upcoming moderators (groups of {y}):\n{blocks}",
    },
    "hr": {
        "help_title": "🧭 Rotacija uloga — Naredbe",
        "help_body": (
            "• **/{cg} help** — Prikaži ovu pomoć.\n"
            "• **/{cg} run** — ▶️ Pokreni jednu rotaciju sada (odaberi sljedećih {y} korisnika). *Samo administratori i dopušteni*.\n"
            "• **/{cg} status** — 📌 Prikaži trenutne nositelje i nadolazeće grupe od {y}.\n"
            "• **/{cg} picks-number show** — 🔢 Prikaži broj odabira (y).\n"
            "• **/{cg} picks-number set** — 🔧 Postavi broj odabira (y). *Samo administratori i dopušteni*.\n"
            "• **/{cg} language show** — 🌐 Prikaži odabrani jezik.\n"
            "• **/{cg} language set** — 🗣️ Promijeni jezik. *Samo administratori i dopušteni*.\n"
            "• **/{cg} permissions add_user/remove_user/add_role/remove_role** — 🛂 Upravlja ovlastima za admin naredbe.\n"
            "• **/{cg} permissions list** — 📜 Prikaži dopuštene korisnike i uloge.\n"
            "• **/{cg} schedule set** — ⏰ Podesi automatsku rotaciju (daily/weekly/monthly/every_n_days).\n"
            "• **/{cg} schedule show** — 📅 Prikaži raspored **i** nadolazeće moderatore.\n"
            "• **/{cg} schedule stop** — 🛑 Isključi automatsku rotaciju.\n"
            "• **/{cg} stats** — 📊 Prikaži ukupno vrijeme svake osobe s ulogom **{b}** (CSV u prilogu)."
        ),
        "not_admin_or_permitted": "⚠️ Morate biti administrator ili imati dopuštenje bota za tu naredbu.",
        "missing_roles": "⚠️ Konfigurirane uloge nisu pronađene. Provjerite nazive **{a}** / **{b}**.",
        "run_start": "🔁 Pokrećem rotaciju…",
        "run_done": "✅ Dodijeljena uloga **{b}**: {mentions}",
        "run_none": "ℹ️ Nema podobnih korisnika s ulogom **{a}**.",
        "status_title": "📌 Status rotacije",
        "status_current": "✅ **Trenutačno imaju {b} ({n}):** {mentions}",
        "status_none_current": "✅ **Trenutačno imaju {b}:** *(nitko)*",
        "status_upcoming": "🔮 **Nadolazeće (grupe od {y}):**\n{blocks}",
        "picks_show": "🔢 Trenutačni broj odabira (y) = **{y}**",
        "picks_set": "🔧 Broj odabira (y) postavljen na **{y}**.",
        "lang_show": "🌐 Trenutačni jezik: **{lang}**",
        "lang_set": "🌐 Jezik promijenjen na **{lang}**.",
        "perm_list": "📜 **Dopušteni korisnici:** {users}\n**Dopuštene uloge:** {roles}",
        "perm_added_user": "🛂 Dodano dopuštenje korisniku: {mention}",
        "perm_removed_user": "🧹 Uklonjeno dopuštenje korisniku: {mention}",
        "perm_added_role": "🛂 Dodano dopuštenje ulozi: **{name}**",
        "perm_removed_role": "🧹 Uklonjeno dopuštenje ulozi: **{name}**",
        "schedule_show_disabled": "🛑 Raspored je **isključen**.",
        "schedule_show_enabled": "📅 Raspored: **{desc}** — sljedeće pokretanje **{when}**.",
        "schedule_set": "✅ Raspored ažuriran: **{desc}** — sljedeće pokretanje **{when}**.",
        "schedule_stopped": "🛑 Automatski raspored je **isključen**.",
        "error": "❌ Greška: {msg}",
        "startup_synced": "✅ Naredbe očišćene i ponovno sinkronizirane.",
        "startup_ready": "🤖 Bot je spreman.",
        "stats_saved": "💾 Statistika spremljena.",
        "stats_title": "📊 Statistika za ulogu **{b}**",
        "stats_none": "🙈 Još nema statistike — nitko nije imao **{b}**.",
        "stats_csv_note": "📎 Cijela statistika u CSV-u u privitku.",
        "schedule_upcoming_mods": "🧑‍⚖️ Nadolazeći moderatori (grupe od {y}):\n{blocks}",
    }
}

def t(guild_id: int, key: str, **kwargs) -> str:
    lang = get_data()["config"].get("language", DEFAULT_LANGUAGE)
    msg = I18N.get(lang, I18N["en"]).get(key, key)
    return msg.format(**kwargs)

# ========== DATA ==========
def default_data() -> Dict:
    return {
        "config": {
            "guild_id": None,
            "role_a_name": ROLE_A_NAME,
            "role_b_name": ROLE_B_NAME,
            "picks_y": DEFAULT_PICKS_Y,
            "language": DEFAULT_LANGUAGE,
            "timezone": DEFAULT_TZ,
            "schedule": DEFAULT_SCHEDULE.copy(),
        },
        "permissions": {"users": [], "roles": []},
        "current_holders": [],
        "stats": {},  # user_id -> { "seconds_total": int, "hold_started_ts": int|None }
        "current_cycle": {"queue": [], "index": 0},
        "next_run_ts": None
    }

_DATA_CACHE = None

def get_data() -> Dict:
    global _DATA_CACHE
    if _DATA_CACHE is None:
        if DATA_FILE.exists():
            try:
                _DATA_CACHE = json.loads(DATA_FILE.read_text(encoding="utf-8"))
            except Exception:
                logger.exception("Failed to read data.json, recreating with defaults.")
                _DATA_CACHE = default_data()
        else:
            _DATA_CACHE = default_data()
    return _DATA_CACHE

def save_data():
    DATA_FILE.write_text(json.dumps(get_data(), indent=2, ensure_ascii=False), encoding="utf-8")
    logger.debug("Data saved.")

# ========== DISCORD CLIENT ==========
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.message_content = False

bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

# dynamic group name
main_group = app_commands.Group(name=COMMAND_GROUP_NAME, description="Role Cycler commands")

# ========== PERMISSIONS ==========
def is_admin_or_permitted(member: discord.Member) -> bool:
    if member.guild_permissions.administrator:
        return True
    data = get_data()
    if member.id in set(data["permissions"]["users"]):
        return True
    allowed_roles = set(data["permissions"]["roles"])
    return any(r.id in allowed_roles for r in member.roles)

# ========== HELPERS ==========
def get_tz() -> ZoneInfo:
    zone = get_data()["config"].get("timezone", DEFAULT_TZ)
    return ZoneInfo(zone)

def parse_hhmm(hhmm: str) -> Tuple[int, int]:
    try:
        h, m = hhmm.split(":")
        return int(h), int(m)
    except Exception:
        return (9, 0)

def find_role_by_name(guild: discord.Guild, name: str) -> Optional[discord.Role]:
    name_lower = name.lower()
    for role in guild.roles:
        if role.name.lower() == name_lower:
            return role
    return None

async def resolve_roles(guild: discord.Guild) -> Tuple[Optional[discord.Role], Optional[discord.Role]]:
    cfg = get_data()["config"]
    return find_role_by_name(guild, cfg["role_a_name"]), find_role_by_name(guild, cfg["role_b_name"])

def human_dt(ts: Optional[int]) -> str:
    if ts is None:
        return "—"
    dt = datetime.fromtimestamp(ts, tz=get_tz())
    return dt.strftime("%Y-%m-%d %H:%M")

def fmt_duration(seconds: int) -> str:
    seconds = max(0, int(seconds))
    d, rem = divmod(seconds, 86400)
    h, rem = divmod(rem, 3600)
    m, _ = divmod(rem, 60)
    parts = []
    if d: parts.append(f"{d}d")
    if h or d: parts.append(f"{h}h")
    parts.append(f"{m}m")
    return " ".join(parts)

def schedule_description(cfg: Dict) -> str:
    s = cfg["schedule"]
    typ = s.get("type")
    time_str = s.get("time", "09:00")
    if typ == "daily":
        return f"daily at {time_str}"
    if typ == "weekly":
        wd = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"][int(s.get("weekday",0))]
        return f"weekly on {wd} at {time_str}"
    if typ == "monthly":
        return f"monthly on day {int(s.get('dom',1))} at {time_str}"
    if typ == "every_n_days":
        return f"every {int(s.get('n_days',3))} day(s) at {time_str}"
    return "unspecified"

def next_run_from_now() -> Optional[int]:
    cfg = get_data()["config"]
    s = cfg["schedule"]
    if not s.get("enabled", False):
        return None
    tz = get_tz()
    now = datetime.now(tz)
    hh, mm = parse_hhmm(s.get("time", "09:00"))
    typ = s.get("type", "every_n_days")
    if typ == "daily":
        cand = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if cand <= now: cand += timedelta(days=1)
        return int(cand.timestamp())
    if typ == "weekly":
        weekday = int(s.get("weekday", 0))
        cand = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        days_ahead = (weekday - cand.weekday()) % 7
        if days_ahead == 0 and cand <= now:
            days_ahead = 7
        cand += timedelta(days=days_ahead)
        return int(cand.timestamp())
    if typ == "monthly":
        dom = int(s.get("dom", 1))
        y, m = now.year, now.month
        cand = now.replace(day=min(dom, 28), hour=hh, minute=mm, second=0, microsecond=0)
        for d in range(28, 32):
            try:
                cand = cand.replace(day=min(dom, d))
            except ValueError:
                continue
        if cand <= now:
            if m == 12: y, m2 = y + 1, 1
            else: y, m2 = y, m + 1
            cand = cand.replace(year=y, month=m2)
            for d in range(28, 32):
                try:
                    cand = cand.replace(day=min(dom, d))
                except ValueError:
                    continue
        return int(cand.timestamp())
    if typ == "every_n_days":
        n = max(1, int(s.get("n_days", 3)))
        cand = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        while cand <= now:
            cand += timedelta(days=n)
        return int(cand.timestamp())
    return None

async def save_and_reschedule():
    get_data()["next_run_ts"] = next_run_from_now()
    save_data()

def chunked(lst: List[int], n: int) -> List[List[int]]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def ensure_user_stat(uid: int):
    stats = get_data()["stats"]
    if str(uid) not in stats:
        stats[str(uid)] = {"seconds_total": 0, "hold_started_ts": None}

def eligible_members_with_role_a(guild: discord.Guild, role_a: discord.Role) -> List[discord.Member]:
    members = [m for m in role_a.members if not m.bot]
    return sorted(members, key=lambda m: m.id)

async def remove_role_b_from_current_holders(guild: discord.Guild, role_b: discord.Role):
    data = get_data()
    current_ids = list(data.get("current_holders", []))
    if not current_ids:
        return
    now = int(datetime.now(tz=get_tz()).timestamp())
    for uid in current_ids:
        member = guild.get_member(uid)
        # finalize stats
        ensure_user_stat(uid)
        stats = data["stats"].get(str(uid))
        if stats and stats.get("hold_started_ts"):
            dur = now - int(stats["hold_started_ts"])
            stats["seconds_total"] = int(stats.get("seconds_total", 0)) + max(0, dur)
            stats["hold_started_ts"] = None
        if member and role_b in member.roles:
            try:
                await member.remove_roles(role_b, reason="Role Cycler: rotation")
                await asyncio.sleep(0.2)
            except Exception:
                logger.exception("Failed removing role B from %s", member)
    data["current_holders"] = []
    save_data()

def shuffle_cycle_queue(members: List[discord.Member]) -> List[int]:
    ids = [m.id for m in members]
    random.shuffle(ids)
    return ids

def repair_cycle_with_current_a(current_queue: List[int], current_index: int, a_ids_now: List[int]) -> Tuple[List[int], int]:
    remaining = current_queue[current_index:]
    remaining = [uid for uid in remaining if uid in a_ids_now]
    return remaining, 0

async def pick_next_batch(guild: discord.Guild, role_a: discord.Role, role_b: discord.Role) -> List[discord.Member]:
    data = get_data()
    cfg = data["config"]
    y = int(cfg.get("picks_y", DEFAULT_PICKS_Y))
    if y < 1: y = 1

    members_a = eligible_members_with_role_a(guild, role_a)
    a_ids_now = [m.id for m in members_a]

    cycle = data["current_cycle"]
    queue = cycle.get("queue", [])
    index = int(cycle.get("index", 0))

    leftovers, _ = repair_cycle_with_current_a(queue, index, a_ids_now)

    if not leftovers:
        newq = shuffle_cycle_queue(members_a)
        queue = newq
        index = 0
        leftovers = []
    else:
        queue = leftovers
        index = 0

    remaining = len(queue) - index
    selected_ids: List[int] = []

    if remaining >= y:
        selected_ids = queue[index:index+y]
        index += y
    else:
        z_take = queue[index:]
        new_cycle_ids = shuffle_cycle_queue(members_a)
        needed = y - len(z_take)
        topup = new_cycle_ids[:needed]
        selected_ids = z_take + topup
        queue = new_cycle_ids[needed:]
        index = 0

    data["current_cycle"]["queue"] = queue
    data["current_cycle"]["index"] = index

    selected_members: List[discord.Member] = []
    map_a = {m.id: m for m in members_a}
    for uid in selected_ids:
        if uid in map_a:
            selected_members.append(map_a[uid])

    now_ts = int(datetime.now(tz=get_tz()).timestamp())
    for m in selected_members:
        try:
            await m.add_roles(role_b, reason="Role Cycler: rotation")
            ensure_user_stat(m.id)
            get_data()["stats"][str(m.id)]["hold_started_ts"] = now_ts
            await asyncio.sleep(0.3)
        except Exception:
            logger.exception("Failed adding role B to %s", m)

    get_data()["current_holders"] = [m.id for m in selected_members]
    save_data()
    return selected_members

async def run_rotation(interaction: discord.Interaction) -> Tuple[List[discord.Member], Optional[str]]:
    guild = interaction.guild
    if guild is None:
        return [], "Guild not found."
    role_a, role_b = await resolve_roles(guild)
    if not role_a or not role_b:
        return [], t(guild.id, "missing_roles", a=get_data()["config"]["role_a_name"], b=get_data()["config"]["role_b_name"])
    await remove_role_b_from_current_holders(guild, role_b)
    selected = await pick_next_batch(guild, role_a, role_b)
    return selected, None

# ========== BACKGROUND SCHEDULER ==========
async def scheduler_loop():
    await bot.wait_until_ready()
    logger.debug("Scheduler loop started.")
    while not bot.is_closed():
        try:
            data = get_data()
            next_ts = data.get("next_run_ts")
            sched = data["config"]["schedule"]
            if not sched.get("enabled", False):
                await asyncio.sleep(10)
                continue
            if next_ts is None:
                await save_and_reschedule()
                await asyncio.sleep(5)
                continue
            now = int(datetime.now(tz=get_tz()).timestamp())
            if now >= next_ts:
                for guild in bot.guilds:
                    if data["config"]["guild_id"] in (None, guild.id):
                        try:
                            role_a, role_b = await resolve_roles(guild)
                            if role_a and role_b:
                                await remove_role_b_from_current_holders(guild, role_b)
                                await pick_next_batch(guild, role_a, role_b)
                        except Exception:
                            logger.exception("Scheduled run failed for guild %s", guild.id)
                await save_and_reschedule()
            else:
                await asyncio.sleep(min(30, max(5, next_ts - now)))
        except Exception:
            logger.exception("Error in scheduler; sleeping.")
            await asyncio.sleep(10)

# ========== COMMANDS ==========

@bot.event
async def on_ready():
    # ensure guild id stored
    if bot.guilds:
        data = get_data()
        if data["config"].get("guild_id") is None:
            data["config"]["guild_id"] = bot.guilds[0].id
            save_data()

    # Clear and re-sync *guild* commands for each guild (fresh every launch)
    try:
        for g in bot.guilds:
            guild_obj = discord.Object(id=g.id)
            tree.clear_commands(guild=guild_obj)           # clear local commands for this guild
            await tree.sync(guild=guild_obj)               # push clear -> removes remote guild commands
            tree.copy_global_to(guild=guild_obj)           # copy global definitions into guild scope
            await tree.sync(guild=guild_obj)               # push fresh set
        logger.info("Guild commands cleared & re-synced for all guilds.")
    except Exception:
        logger.exception("Failed to clear/resync guild commands.")

    logger.info("Bot ready.")
    # schedule next run based on current config
    await save_and_reschedule()
    bot.loop.create_task(scheduler_loop())

# ---- /commands_starter help ----
@main_group.command(name="help", description="Show help for all commands")
async def cmd_help(interaction: discord.Interaction):
    data = get_data()
    y = data["config"].get("picks_y", DEFAULT_PICKS_Y)
    embed = discord.Embed(
        title=t(interaction.guild_id, "help_title"),
        description=t(interaction.guild_id, "help_body", cg=COMMAND_GROUP_NAME, y=y, b=get_data()["config"]["role_b_name"]),
        colour=discord.Colour.blurple()
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)
    logger.debug("/help invoked by %s", interaction.user)

# ---- /commands_starter run ----
@main_group.command(name="run", description="Run one rotation now (restricted)")
async def cmd_run(interaction: discord.Interaction):
    if not is_admin_or_permitted(interaction.user):
        await interaction.response.send_message(t(interaction.guild_id, "not_admin_or_permitted"), ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True, thinking=True)
    try:
        selected, err = await run_rotation(interaction)
        if err:
            await interaction.followup.send(t(interaction.guild_id, "error", msg=err), ephemeral=True)
            return
        if not selected:
            await interaction.followup.send(t(interaction.guild_id, "run_none", a=get_data()["config"]["role_a_name"]), ephemeral=True)
            return
        mentions = ", ".join(m.mention for m in selected)
        await interaction.followup.send(t(interaction.guild_id, "run_done", b=get_data()["config"]["role_b_name"], mentions=mentions), ephemeral=False)
    except Exception as e:
        logger.exception("Error in /run")
        await interaction.followup.send(t(interaction.guild_id, "error", msg=str(e)), ephemeral=True)

# ---- /commands_starter status ----
@main_group.command(name="status", description="Show current holders and upcoming order")
async def cmd_status(interaction: discord.Interaction):
    data = get_data()
    cfg = data["config"]
    y = int(cfg.get("picks_y", DEFAULT_PICKS_Y))
    bname = cfg.get("role_b_name", ROLE_B_NAME)
    guild = interaction.guild

    current_ids = data.get("current_holders", [])
    current_members = [guild.get_member(uid) for uid in current_ids]
    current_mentions = [m.mention for m in current_members if m is not None]

    queue = list(data["current_cycle"].get("queue", []))
    index = int(data["current_cycle"].get("index", 0))
    upcoming_ids = queue[index:] if index < len(queue) else []
    blocks = []
    for group in chunked(upcoming_ids, y):
        line = " • " + ", ".join(f"<@{uid}>" for uid in group)
        blocks.append(line)
    upcoming_text = "\n".join(blocks) if blocks else " *(no upcoming — new cycle will start)*"

    embed = discord.Embed(title=t(interaction.guild_id, "status_title"), colour=discord.Colour.green())
    if current_mentions:
        embed.add_field(name="\u200b", value=t(interaction.guild_id, "status_current", b=bname, n=len(current_mentions), mentions=", ".join(current_mentions)), inline=False)
    else:
        embed.add_field(name="\u200b", value=t(interaction.guild_id, "status_none_current", b=bname), inline=False)
    embed.add_field(name="\u200b", value=t(interaction.guild_id, "status_upcoming", y=y, blocks=upcoming_text), inline=False)

    await interaction.response.send_message(embed=embed, ephemeral=False)
    logger.debug("/status invoked by %s", interaction.user)

# ---- /commands_starter picks-number show|set ----
picks_group = app_commands.Group(name="picks-number", description="Show or set picks (y)")

@picks_group.command(name="show", description="Show current picks (y)")
async def picks_show(interaction: discord.Interaction):
    y = get_data()["config"].get("picks_y", DEFAULT_PICKS_Y)
    await interaction.response.send_message(t(interaction.guild_id, "picks_show", y=y), ephemeral=True)
    logger.debug("/picks-number show by %s", interaction.user)

@picks_group.command(name="set", description="Set picks (y)")
@app_commands.describe(num="Number of users to pick each rotation (y)")
async def picks_set(interaction: discord.Interaction, num: app_commands.Range[int, 1, 500]):
    if not is_admin_or_permitted(interaction.user):
        await interaction.response.send_message(t(interaction.guild_id, "not_admin_or_permitted"), ephemeral=True)
        return
    get_data()["config"]["picks_y"] = int(num)
    save_data()
    await interaction.response.send_message(t(interaction.guild_id, "picks_set", y=num), ephemeral=True)
    logger.debug("/picks-number set=%s by %s", num, interaction.user)

# ---- /commands_starter language show|set ----
lang_group = app_commands.Group(name="language", description="Show or set language")

@lang_group.command(name="show", description="Show current language")
async def lang_show(interaction: discord.Interaction):
    lang = get_data()["config"].get("language", DEFAULT_LANGUAGE)
    await interaction.response.send_message(t(interaction.guild_id, "lang_show", lang=lang), ephemeral=True)

@lang_group.command(name="set", description="Set language (en/hr)")
@app_commands.describe(language="Choose 'en' (English) or 'hr' (Croatian)")
@app_commands.choices(language=[
    app_commands.Choice(name="English", value="en"),
    app_commands.Choice(name="Hrvatski", value="hr"),
])
async def lang_set(interaction: discord.Interaction, language: app_commands.Choice[str]):
    if not is_admin_or_permitted(interaction.user):
        await interaction.response.send_message(t(interaction.guild_id, "not_admin_or_permitted"), ephemeral=True)
        return
    get_data()["config"]["language"] = language.value
    save_data()
    await interaction.response.send_message(t(interaction.guild_id, "lang_set", lang=language.value), ephemeral=True)
    logger.debug("/language set=%s by %s", language.value, interaction.user)

# ---- /commands_starter permissions ... ----
perm_group = app_commands.Group(name="permissions", description="Manage who can run restricted commands")

@perm_group.command(name="list", description="List permitted users & roles")
async def perm_list(interaction: discord.Interaction):
    data = get_data()
    users = data["permissions"]["users"]
    roles = data["permissions"]["roles"]
    users_text = ", ".join(f"<@{u}>" for u in users) if users else "—"
    roles_text = ", ".join(f"<@&{r}>" for r in roles) if roles else "—"
    await interaction.response.send_message(t(interaction.guild_id, "perm_list", users=users_text, roles=roles_text), ephemeral=True)
    logger.debug("/permissions list by %s", interaction.user)

@perm_group.command(name="add_user", description="Allow a user to run restricted commands")
async def perm_add_user(interaction: discord.Interaction, user: discord.Member):
    if not is_admin_or_permitted(interaction.user):
        await interaction.response.send_message(t(interaction.guild_id, "not_admin_or_permitted"), ephemeral=True)
        return
    arr = get_data()["permissions"]["users"]
    if user.id not in arr:
        arr.append(user.id)
        save_data()
    await interaction.response.send_message(t(interaction.guild_id, "perm_added_user", mention=user.mention), ephemeral=True)
    logger.debug("/permissions add_user %s by %s", user, interaction.user)

@perm_group.command(name="remove_user", description="Remove a user's permission")
async def perm_remove_user(interaction: discord.Interaction, user: discord.Member):
    if not is_admin_or_permitted(interaction.user):
        await interaction.response.send_message(t(interaction.guild_id, "not_admin_or_permitted"), ephemeral=True)
        return
    arr = get_data()["permissions"]["users"]
    if user.id in arr:
        arr.remove(user.id)
        save_data()
    await interaction.response.send_message(t(interaction.guild_id, "perm_removed_user", mention=user.mention), ephemeral=True)
    logger.debug("/permissions remove_user %s by %s", user, interaction.user)

@perm_group.command(name="add_role", description="Allow a role to run restricted commands")
async def perm_add_role(interaction: discord.Interaction, role: discord.Role):
    if not is_admin_or_permitted(interaction.user):
        await interaction.response.send_message(t(interaction.guild_id, "not_admin_or_permitted"), ephemeral=True)
        return
    arr = get_data()["permissions"]["roles"]
    if role.id not in arr:
        arr.append(role.id)
        save_data()
    await interaction.response.send_message(t(interaction.guild_id, "perm_added_role", name=role.name), ephemeral=True)
    logger.debug("/permissions add_role %s by %s", role, interaction.user)

@perm_group.command(name="remove_role", description="Remove a role permission")
async def perm_remove_role(interaction: discord.Interaction, role: discord.Role):
    if not is_admin_or_permitted(interaction.user):
        await interaction.response.send_message(t(interaction.guild_id, "not_admin_or_permitted"), ephemeral=True)
        return
    arr = get_data()["permissions"]["roles"]
    if role.id in arr:
        arr.remove(role.id)
        save_data()
    await interaction.response.send_message(t(interaction.guild_id, "perm_removed_role", name=role.name), ephemeral=True)
    logger.debug("/permissions remove_role %s by %s", role, interaction.user)

# ---- /commands_starter schedule ... ----
schedule_group = app_commands.Group(name="schedule", description="Automate the rotation")

@schedule_group.command(name="show", description="Show current schedule and upcoming moderators")
async def schedule_show(interaction: discord.Interaction):
    cfg = get_data()["config"]
    bname = cfg.get("role_b_name", ROLE_B_NAME)
    y = int(cfg.get("picks_y", DEFAULT_PICKS_Y))

    if not cfg["schedule"].get("enabled", False):
        await interaction.response.send_message(t(interaction.guild_id, "schedule_show_disabled"), ephemeral=True)
        return

    desc = schedule_description(cfg)
    nxt = get_data().get("next_run_ts")
    when = human_dt(nxt) if nxt else "—"

    # Build upcoming moderators list (to end of current queue)
    data = get_data()
    queue = list(data["current_cycle"].get("queue", []))
    index = int(data["current_cycle"].get("index", 0))
    upcoming_ids = queue[index:] if index < len(queue) else []
    blocks = []
    for group in chunked(upcoming_ids, y):
        line = " • " + ", ".join(f"<@{uid}>" for uid in group)
        blocks.append(line)
    upcoming_text = "\n".join(blocks) if blocks else " *(no upcoming — new cycle will start)*"

    embed = discord.Embed(colour=discord.Colour.blurple())
    embed.add_field(name="📅", value=t(interaction.guild_id, "schedule_show_enabled", desc=desc, when=when), inline=False)
    embed.add_field(name="🧑‍⚖️", value=t(interaction.guild_id, "schedule_upcoming_mods", y=y, blocks=upcoming_text), inline=False)

    await interaction.response.send_message(embed=embed, ephemeral=False)

@schedule_group.command(name="stop", description="Disable automatic rotation (visible to everyone)")
async def schedule_stop(interaction: discord.Interaction):
    # Now usable by everyone (per your request)
    get_data()["config"]["schedule"]["enabled"] = False
    get_data()["next_run_ts"] = None
    save_data()
    await interaction.response.send_message(t(interaction.guild_id, "schedule_stopped"), ephemeral=True)
    logger.debug("/schedule stop by %s", interaction.user)

@schedule_group.command(name="set", description="Set schedule (daily/weekly/monthly/every_n_days) — visible to everyone")
@app_commands.describe(
    kind="Frequency kind",
    time="Time HH:MM in server timezone",
    n_days="Used when kind = every_n_days",
    weekday="0=Mon .. 6=Sun (for weekly)",
    day_of_month="1..31 (for monthly)"
)
@app_commands.choices(kind=[
    app_commands.Choice(name="daily", value="daily"),
    app_commands.Choice(name="weekly", value="weekly"),
    app_commands.Choice(name="monthly", value="monthly"),
    app_commands.Choice(name="every_n_days", value="every_n_days"),
])
async def schedule_set(
    interaction: discord.Interaction,
    kind: app_commands.Choice[str],
    time: str = "00:00",
    n_days: Optional[app_commands.Range[int, 1, 60]] = None,
    weekday: Optional[app_commands.Range[int, 0, 6]] = None,
    day_of_month: Optional[app_commands.Range[int, 1, 31]] = None
):
    # Now usable by everyone (per your request)
    s = get_data()["config"]["schedule"]
    s["type"] = kind.value
    s["time"] = time
    s["enabled"] = True
    if kind.value == "every_n_days":
        s["n_days"] = int(n_days or 3)
    elif kind.value == "weekly":
        s["weekday"] = int(weekday if weekday is not None else 0)
    elif kind.value == "monthly":
        s["dom"] = int(day_of_month if day_of_month is not None else 1)

    await save_and_reschedule()
    desc = schedule_description(get_data()["config"])
    when = human_dt(get_data().get("next_run_ts"))
    await interaction.response.send_message(t(interaction.guild_id, "schedule_set", desc=desc, when=when), ephemeral=True)
    logger.debug("/schedule set %s by %s", s, interaction.user)

# ---- /commands_starter stats ----
@main_group.command(name="stats", description="Show total time each user held role B")
async def stats_cmd(interaction: discord.Interaction):
    data = get_data()
    cfg = data["config"]
    bname = cfg.get("role_b_name", ROLE_B_NAME)
    stats = data.get("stats", {})
    guild = interaction.guild
    now = int(datetime.now(tz=get_tz()).timestamp())

    if not stats:
        await interaction.response.send_message(t(interaction.guild_id, "stats_none", b=bname), ephemeral=True)
        return

    # Build rows: (total_seconds_including_current, uid, display_name)
    rows = []
    for uid_str, rec in stats.items():
        uid = int(uid_str)
        total = int(rec.get("seconds_total", 0))
        start = rec.get("hold_started_ts")
        if start:
            total += max(0, now - int(start))
        member = guild.get_member(uid)
        name = member.display_name if member else f"User {uid}"
        rows.append((total, uid, name))

    rows.sort(reverse=True, key=lambda r: r[0])

    # Embed with top lines + attach CSV for full data
    embed = discord.Embed(title=t(interaction.guild_id, "stats_title", b=bname), colour=discord.Colour.gold())

    # Format up to ~40 lines safely
    lines = [f"{i+1}. <@{uid}> — ⏱️ {fmt_duration(total)}" for i, (total, uid, _) in enumerate(rows)]
    chunks = []
    chunk = []
    char_count = 0
    for line in lines:
        if char_count + len(line) + 1 > 1000:  # keep under field limit
            chunks.append("\n".join(chunk) if chunk else "—")
            chunk, char_count = [], 0
        chunk.append(line)
        char_count += len(line) + 1
    if chunk:
        chunks.append("\n".join(chunk))

    # Add up to 3 fields to avoid very large embeds; CSV covers the rest
    for idx, block in enumerate(chunks[:3]):
        embed.add_field(name=f"Page {idx+1}", value=block, inline=False)

    embed.set_footer(text=t(interaction.guild_id, "stats_csv_note"))

    # build CSV
    csv_buf = io.StringIO()
    writer = csv.writer(csv_buf)
    writer.writerow(["rank", "user_id", "display_name", "total_seconds", "human_total"])
    for i, (total, uid, name) in enumerate(rows, start=1):
        writer.writerow([i, uid, name, total, fmt_duration(total)])
    csv_buf.seek(0)
    file = discord.File(fp=io.BytesIO(csv_buf.getvalue().encode("utf-8")), filename="role_b_stats.csv")

    await interaction.response.send_message(embed=embed, file=file, ephemeral=False)
    logger.debug("/stats invoked by %s", interaction.user)

# Register sub-groups to main group
main_group.add_command(picks_group)
main_group.add_command(lang_group)
main_group.add_command(perm_group)
main_group.add_command(schedule_group)

# Add main group to command tree
tree.add_command(main_group)

# ========== ENTRY ==========
if __name__ == "__main__":
    if not DISCORD_TOKEN:
        logger.error("Environment variable DISCORD_TOKEN is not set.")
        print("Set DISCORD_TOKEN in your environment before running.")
    else:
        bot.run(DISCORD_TOKEN)
