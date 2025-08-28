# -----------------------------
# Role Cycler Discord Bot (slash-commands)
# -----------------------------
# Requirements at top (request): settable defaults for:
#   - interval x (schedule), picks y, role_a name, role_b name, command group name
#   - plus default language and timezone for scheduling
#
# Readability & logging emphasized; JSON persistence used for all state.
# -----------------------------

import os
import sys
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

# ========== USER-TUNABLE DEFAULTS (can be changed later via slash commands) ==========
DEFAULT_PICKS_Y = 1                         # y: how many users get role B each run
DEFAULT_SCHEDULE = {                        # x: default schedule (editable later)
    "enabled": False,
    "type": "every_n_days",                 # "daily" | "weekly" | "monthly" | "every_n_days"
    "n_days": 3,                            # used when type == "every_n_days"
    "weekday": 0,                           # Monday=0..Sunday=6 (used when type == "weekly")
    "dom": 1,                               # day of month 1..28/29/30/31 (used when type == "monthly")
    "time": "09:00"                         # HH:MM (24h) in DEFAULT_TZ
}
ROLE_A_NAME = "a"                           # a: members must have this to be eligible
ROLE_B_NAME = "b"                           # b: this is the rotating role
COMMAND_GROUP_NAME = "khan"     # command starter for group: /commands_starter ...
DEFAULT_LANGUAGE = "en"                     # "en" or "hr"
DEFAULT_TZ = "Europe/Zagreb"                # used for scheduler and timestamps

# ========== FILES / ENV ==========
DATA_FILE = Path("data.json")
LOG_FILE = Path("bot.log")
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")  # Put your token in environment variable DISCORD_TOKEN

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

# Pipe discord's own loggers into our handlers too
for _name in ("discord", "discord.app_commands"):
    _dlog = logging.getLogger(_name)
    _dlog.setLevel(logging.DEBUG)
    _dlog.addHandler(sh)
    _dlog.addHandler(fh)
    _dlog.propagate = False  # avoid double-printing

# Make absolutely sure uncaught stuff is written to bot.log
def _excepthook(exc_type, exc, tb):
    logger.exception("Uncaught exception", exc_info=(exc_type, exc, tb))
sys.excepthook = _excepthook

# ========== I18N ==========
I18N = {
    "en": {
        "help_title": "Role Cycler — Commands",
        "help_body": (
            "• **/{cg} help** — Show this help.\n"
            "• **/{cg} run** — Run one rotation now (pick next {y} users). *Admins & permitted only*.\n"
            "• **/{cg} status** — Show current holders and the upcoming schedule in groups of {y}.\n"
            "• **/{cg} picks-number show** — Show the current number of picks (y).\n"
            "• **/{cg} picks-number set** — Set the number of picks (y). *Admins & permitted only*.\n"
            "• **/{cg} language show** — Show selected language.\n"
            "• **/{cg} language set** — Change language. *Admins & permitted only*.\n"
            "• **/{cg} permissions add_user/remove_user/add_role/remove_role** — Manage who may run admin commands.\n"
            "• **/{cg} permissions list** — List permitted users and roles.\n"
            "• **/{cg} schedule set** — Configure automatic rotation (daily/weekly/monthly/every_n_days). *Admins & permitted only*.\n"
            "• **/{cg} schedule show** — Show current schedule.\n"
            "• **/{cg} schedule stop** — Disable automatic rotation. *Admins & permitted only*."
        ),
        "not_admin_or_permitted": "You must be an administrator or permitted by this bot to use that command.",
        "missing_roles": "Configured roles not found. Please check the role names in settings.",
        "run_start": "Starting rotation…",
        "run_done": ":crown: Assigned **{b}** to: {mentions}",
        "run_none": "No eligible users with role **{a}** were found.",
        "status_title": "Rotation Status",
        "status_current": "**Currently holding {b} ({n}):** {mentions}",
        "status_none_current": "**Currently holding {b}:** *(none)*",
        "status_upcoming": "**Upcoming (groups of {y}):**\n{blocks}",
        "picks_show": "Current picks (y) = **{y}**",
        "picks_set": "Updated picks (y) to **{y}**.",
        "lang_show": "Current language: **{lang}**",
        "lang_set": "Language updated to **{lang}**.",
        "perm_list": "**Permitted Users:** {users}\n**Permitted Roles:** {roles}",
        "perm_added_user": "Added user permission: {mention}",
        "perm_removed_user": "Removed user permission: {mention}",
        "perm_added_role": "Added role permission: **{name}**",
        "perm_removed_role": "Removed role permission: **{name}**",
        "schedule_show_disabled": "Schedule is **disabled**.",
        "schedule_show_enabled": "Schedule: **{desc}** — next run at **{when}**.",
        "schedule_set": "Schedule updated: **{desc}** — next run at **{when}**.",
        "schedule_stopped": "Automatic schedule **disabled**.",
        "error": "Error: {msg}",
        "startup_synced": "Commands synced.",
        "startup_ready": "Bot is ready.",
        "stats_saved": "Saved stats.",
    },
    "hr": {
        "help_title": "Rotacija uloga — Naredbe",
        "help_body": (
            "• **/{cg} help** — Prikaži ovu pomoć.\n"
            "• **/{cg} run** — Pokreni jednu rotaciju sada (odaberi sljedećih {y} korisnika). *Samo administratori i dopušteni*.\n"
            "• **/{cg} status** — Prikaži trenutne nositelje i nadolazeći redoslijed u grupama od {y}.\n"
            "• **/{cg} picks-number show** — Prikaži trenutačni broj odabira (y).\n"
            "• **/{cg} picks-number set** — Postavi broj odabira (y). *Samo administratori i dopušteni*.\n"
            "• **/{cg} language show** — Prikaži odabrani jezik.\n"
            "• **/{cg} language set** — Promijeni jezik. *Samo administratori i dopušteni*.\n"
            "• **/{cg} permissions add_user/remove_user/add_role/remove_role** — Upravlja ovlastima za pokretanje admin naredbi.\n"
            "• **/{cg} permissions list** — Prikaži dopuštene korisnike i uloge.\n"
            "• **/{cg} schedule set** — Podesi automatsku rotaciju (daily/weekly/monthly/every_n_days). *Samo administratori i dopušteni*.\n"
            "• **/{cg} schedule show** — Prikaži trenutačni raspored.\n"
            "• **/{cg} schedule stop** — Isključi automatsku rotaciju. *Samo administratori i dopušteni*."
        ),
        "not_admin_or_permitted": "Morate biti administrator ili imati dopuštenje bota za korištenje te naredbe.",
        "missing_roles": "Konfigurirane uloge nisu pronađene. Provjerite nazive uloga u postavkama.",
        "run_start": "Pokrećem rotaciju…",
        "run_done": ":crown: Dodijeljena uloga **{b}** za: {mentions}",
        "run_none": "Nema podobnih korisnika s ulogom **{a}**.",
        "status_title": "Status rotacije",
        "status_current": "**Trenutačno imaju {b} ({n}):** {mentions}",
        "status_none_current": "**Trenutačno imaju {b}:** *(nitko)*",
        "status_upcoming": "**Nadolazeće (grupe od {y}):**\n{blocks}",
        "picks_show": "Trenutačni broj odabira (y) = **{y}**",
        "picks_set": "Broj odabira (y) postavljen na **{y}**.",
        "lang_show": "Trenutačni jezik: **{lang}**",
        "lang_set": "Jezik promijenjen na **{lang}**.",
        "perm_list": "**Dopušteni korisnici:** {users}\n**Dopuštene uloge:** {roles}",
        "perm_added_user": "Dodano dopuštenje korisniku: {mention}",
        "perm_removed_user": "Uklonjeno dopuštenje korisniku: {mention}",
        "perm_added_role": "Dodano dopuštenje ulozi: **{name}**",
        "perm_removed_role": "Uklonjeno dopuštenje ulozi: **{name}**",
        "schedule_show_disabled": "Raspored je **isključen**.",
        "schedule_show_enabled": "Raspored: **{desc}** — sljedeće pokretanje u **{when}**.",
        "schedule_set": "Raspored ažuriran: **{desc}** — sljedeće pokretanje u **{when}**.",
        "schedule_stopped": "Automatski raspored je **isključen**.",
        "error": "Greška: {msg}",
        "startup_synced": "Naredbe sinkronizirane.",
        "startup_ready": "Bot je spreman.",
        "stats_saved": "Statistika spremljena.",
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
        "permissions": {
            "users": [],   # user IDs allowed to use restricted commands
            "roles": []    # role IDs allowed to use restricted commands
        },
        "current_holders": [],  # user IDs who currently have role B
        "stats": {               # user_id -> { "seconds_total": int, "hold_started_ts": int|None }
        },
        "current_cycle": {
            "queue": [],         # list of user IDs (order)
            "index": 0           # pointer to next position in queue
        },
        "next_run_ts": None      # for scheduler
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
intents.members = True  # IMPORTANT: enable "Server Members Intent" in the Discord Developer Portal!
intents.message_content = False

bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

# dynamic group name
main_group = app_commands.Group(name=COMMAND_GROUP_NAME, description="Role Cycler commands")

from discord import app_commands

@tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    # Full traceback to bot.log
    logger.exception("App command error (%s) by %s",
                     getattr(interaction.command, "name", "unknown"), interaction.user, exc_info=error)
    # Best-effort user notice
    try:
        msg = t(interaction.guild_id, "error", msg=str(error))
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)
    except Exception:
        logger.exception("Failed to send error reply")


# ========== PERMISSIONS ==========
def is_admin_or_permitted(member: discord.Member) -> bool:
    if member.guild_permissions.administrator:
        return True
    data = get_data()
    if member.id in set(data["permissions"]["users"]):
        return True
    allowed_roles = set(data["permissions"]["roles"])
    for r in member.roles:
        if r.id in allowed_roles:
            return True
    return False

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
    a = find_role_by_name(guild, cfg["role_a_name"])
    b = find_role_by_name(guild, cfg["role_b_name"])
    return a, b

def human_dt(ts: Optional[int]) -> str:
    if ts is None:
        return "—"
    dt = datetime.fromtimestamp(ts, tz=get_tz())
    return dt.strftime("%Y-%m-%d %H:%M")

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
        if cand <= now:
            cand += timedelta(days=1)
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
        # naive month increment
        y, m = now.year, now.month
        cand = now.replace(day=min(dom, 28), hour=hh, minute=mm, second=0, microsecond=0)
        # adjust up to correct day if possible (handles 29/30/31)
        for d in range(28, 32):
            try:
                cand = cand.replace(day=min(dom, d))
            except ValueError:
                continue
        if cand <= now:
            # next month
            if m == 12:
                y, m2 = y + 1, 1
            else:
                y, m2 = y, m + 1
            cand = cand.replace(year=y, month=m2)
            # fix day again
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
    # Ensure unique & stable order before shuffle for reproducibility
    return sorted(members, key=lambda m: m.id)

async def remove_role_b_from_current_holders(guild: discord.Guild, role_b: discord.Role):
    data = get_data()
    current_ids = list(data.get("current_holders", []))
    if not current_ids:
        return
    now = int(datetime.now(tz=get_tz()).timestamp())
    for uid in current_ids:
        member = guild.get_member(uid)
        if member is None:
            # user left; still finalize stats
            stats = data["stats"].get(str(uid))
            if stats and stats.get("hold_started_ts"):
                dur = now - int(stats["hold_started_ts"])
                stats["seconds_total"] = int(stats.get("seconds_total", 0)) + max(0, dur)
                stats["hold_started_ts"] = None
            continue
        # finalize stats
        ensure_user_stat(uid)
        stats = data["stats"][str(uid)]
        if stats.get("hold_started_ts"):
            dur = now - int(stats["hold_started_ts"])
            stats["seconds_total"] = int(stats.get("seconds_total", 0)) + max(0, dur)
            stats["hold_started_ts"] = None
        # remove role B if still present
        if role_b in member.roles:
            try:
                await member.remove_roles(role_b, reason="Role Cycler: rotation")
                await asyncio.sleep(0.2)  # gentle on rate limits
            except Exception:
                logger.exception("Failed removing role B from %s", member)
    data["current_holders"] = []
    save_data()

def shuffle_cycle_queue(members: List[discord.Member]) -> List[int]:
    ids = [m.id for m in members]
    random.shuffle(ids)
    return ids

def repair_cycle_with_current_a(current_queue: List[int], current_index: int, a_ids_now: List[int]) -> Tuple[List[int], int]:
    # Keep remaining portion of queue that still exist in role A
    remaining = current_queue[current_index:]
    remaining = [uid for uid in remaining if uid in a_ids_now]
    # Users who are in A but not in remaining will appear in next cycle
    return remaining, 0  # we treat 'remaining' as the start of a "partial leftovers" list

async def pick_next_batch(guild: discord.Guild, role_a: discord.Role, role_b: discord.Role) -> List[discord.Member]:
    """
    Implements the selection logic with 'cycle' semantics and z<y top-up.
    """
    data = get_data()
    cfg = data["config"]
    y = int(cfg.get("picks_y", DEFAULT_PICKS_Y))
    if y < 1:
        y = 1

    # Build fresh eligible list
    members_a = eligible_members_with_role_a(guild, role_a)
    a_ids_now = [m.id for m in members_a]

    # Prepare cycle state
    cycle = data["current_cycle"]
    queue = cycle.get("queue", [])
    index = int(cycle.get("index", 0))

    # Clean up queue vs current A
    leftovers, _ = repair_cycle_with_current_a(queue, index, a_ids_now)

    # If queue is empty or everyone consumed, (re)build from current A
    if not leftovers:
        newq = shuffle_cycle_queue(members_a)
        queue = newq
        index = 0
        leftovers = []
    else:
        queue = leftovers
        index = 0

    # Decide selection handling z<y case
    remaining = len(queue) - index
    selected_ids: List[int] = []

    if remaining >= y:
        selected_ids = queue[index:index+y]
        index += y
    else:
        # take z
        z_take = queue[index:]
        # start a new cycle
        new_cycle_ids = shuffle_cycle_queue(members_a)
        needed = y - len(z_take)
        topup = new_cycle_ids[:needed]
        selected_ids = z_take + topup
        # new queue becomes: remaining of new cycle after topup
        queue = new_cycle_ids[needed:]
        index = 0

    # Update stored cycle
    data["current_cycle"]["queue"] = queue
    data["current_cycle"]["index"] = index

    # Convert selected IDs to members (filter out non-present)
    selected_members: List[discord.Member] = []
    selected_set = set(selected_ids)
    # members_a is current eligible; use map for speed
    map_a = {m.id: m for m in members_a}
    for uid in selected_ids:
        if uid in map_a:
            selected_members.append(map_a[uid])

    # Assign role B
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
    # Remove role from previous holders and finalize time
    await remove_role_b_from_current_holders(guild, role_b)
    # Pick next batch and assign
    selected = await pick_next_batch(guild, role_a, role_b)
    return selected, None

RUN_LOCKS: dict[int, asyncio.Lock] = {}

def get_run_lock(guild_id: int) -> asyncio.Lock:
    lock = RUN_LOCKS.get(guild_id)
    if lock is None:
        lock = asyncio.Lock()
        RUN_LOCKS[guild_id] = lock
    return lock


# ========== BACKGROUND SCHEDULER ==========

async def scheduler_loop():
    await bot.wait_until_ready()
    logger.debug("Scheduler loop started.")

    def get_announce_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
        """Pick a channel to post scheduler messages to."""
        # 1) Optional configured channel id in data.json: config.announce_channel_id
        cfg_chan_id = get_data()["config"].get("announce_channel_id")
        if cfg_chan_id:
            ch = guild.get_channel(int(cfg_chan_id))
            if isinstance(ch, discord.TextChannel) and ch.permissions_for(guild.me).send_messages:
                return ch
        # 2) Guild system channel
        ch = guild.system_channel
        if isinstance(ch, discord.TextChannel) and ch.permissions_for(guild.me).send_messages:
            return ch
        # 3) First text channel we can send to
        for ch in guild.text_channels:
            if ch.permissions_for(guild.me).send_messages:
                return ch
        return None

    while not bot.is_closed():
        try:
            data = get_data()
            next_ts = data.get("next_run_ts")
            sched = data["config"]["schedule"]

            if not sched.get("enabled", False):
                await asyncio.sleep(10)
                continue

            if next_ts is None:
                # compute and save
                await save_and_reschedule()
                await asyncio.sleep(5)
                continue

            now = int(datetime.now(tz=get_tz()).timestamp())
            if now >= next_ts:
                # run rotation in all guilds where configured (single-guild design)
                for guild in bot.guilds:
                    if data["config"]["guild_id"] in (None, guild.id):
                        try:
                            logger.info("Scheduled run for guild %s", guild.id)
                            lock = get_run_lock(guild.id)

                            async with lock:
                                role_a, role_b = await resolve_roles(guild)
                                if role_a and role_b:
                                    await remove_role_b_from_current_holders(guild, role_b)
                                    await pick_next_batch(guild, role_a, role_b)
                            # role_a, role_b = await resolve_roles(guild)
                            channel = get_announce_channel(guild)

                            if not role_a or not role_b:
                                if channel:
                                    await channel.send(
                                        t(guild.id, "missing_roles",
                                          a=get_data()["config"]["role_a_name"],
                                          b=get_data()["config"]["role_b_name"])
                                    )
                                continue

                            # Do the rotation
                            await remove_role_b_from_current_holders(guild, role_b)
                            selected = await pick_next_batch(guild, role_a, role_b)

                            # Announce like /run does
                            if channel:
                                if selected:
                                    mentions = ", ".join(m.mention for m in selected)
                                    await channel.send(
                                        t(guild.id, "run_done",
                                          b=get_data()["config"]["role_b_name"],
                                          mentions=mentions)
                                    )
                                else:
                                    await channel.send(
                                        t(guild.id, "run_none",
                                          a=get_data()["config"]["role_a_name"])
                                    )

                        except Exception as e:
                            logger.exception("Scheduled run failed for guild %s", guild.id)
                            channel = get_announce_channel(guild)
                            if channel:
                                await channel.send(t(guild.id, "error", msg=str(e)))

                # compute next run
                await save_and_reschedule()

            else:
                await asyncio.sleep(min(30, max(5, next_ts - now)))

        except Exception:
            logger.exception("Error in scheduler; sleeping a bit.")
            await asyncio.sleep(10)
            
# ========== COMMANDS ==========

@bot.event
async def on_ready():
    try:
        if bot.guilds:
            g = bot.guilds[0]
            # Copy globals to the guild and sync there (fast), then also sync global.
            bot.tree.copy_global_to(guild=g)
            await bot.tree.sync(guild=g)
            logger.info("Slash commands synced to guild %s.", g.id)
        # (optional) Also keep a global sync so commands exist everywhere the bot is invited.
        await bot.tree.sync()
        logger.info("Global slash commands synced.")
    except Exception:
        logger.exception("Failed to sync commands.")
    logger.info("Bot ready.")
    bot.loop.create_task(scheduler_loop())
    

# ---- /commands_starter help ----
@main_group.command(name="help", description="Show help for all commands")
async def cmd_help(interaction: discord.Interaction):
    data = get_data()
    y = data["config"].get("picks_y", DEFAULT_PICKS_Y)
    embed = discord.Embed(
        title=t(interaction.guild_id, "help_title"),
        description=t(interaction.guild_id, "help_body", cg=COMMAND_GROUP_NAME, y=y),
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

    # ACK immediately to avoid the 3s race
    try:
        await interaction.response.send_message(t(interaction.guild_id, "run_start"), ephemeral=True)
    except Exception:
        # If somehow already acknowledged, we’ll continue and rely on followups
        logger.exception("Failed to send initial /run ACK")

    lock = get_run_lock(interaction.guild_id or 0)
    async with lock:
        try:
            selected, err = await run_rotation(interaction)
            if err:
                # Prefer editing the ephemeral "Starting..." message; fallback to followup
                try:
                    await interaction.edit_original_response(content=t(interaction.guild_id, "error", msg=err))
                except Exception:
                    await interaction.followup.send(t(interaction.guild_id, "error", msg=err), ephemeral=True)
                return

            if not selected:
                msg = t(interaction.guild_id, "run_none", a=get_data()["config"]["role_a_name"])
                try:
                    await interaction.edit_original_response(content=msg)
                except Exception:
                    await interaction.followup.send(msg, ephemeral=True)
                return

            mentions = ", ".join(m.mention for m in selected)
            public_msg = t(interaction.guild_id, "run_done",
                           b=get_data()["config"]["role_b_name"], mentions=mentions)

            # Update the ephemeral original so the caller sees completion,
            # and also post a public summary message for the server.
            try:
                await interaction.edit_original_response(content=public_msg)
            except Exception:
                pass
            await interaction.followup.send(public_msg, ephemeral=False)

        except Exception as e:
            logger.exception("Error in /run")
            # Try to notify the invoker either by editing or followup
            try:
                await interaction.edit_original_response(content=t(interaction.guild_id, "error", msg=str(e)))
            except Exception:
                try:
                    await interaction.followup.send(t(interaction.guild_id, "error", msg=str(e)), ephemeral=True)
                except Exception:
                    pass


# ---- /commands_starter status ----
@main_group.command(name="status", description="Show current holders and upcoming order")
async def cmd_status(interaction: discord.Interaction):
    data = get_data()
    cfg = data["config"]
    y = int(cfg.get("picks_y", DEFAULT_PICKS_Y))
    bname = cfg.get("role_b_name", ROLE_B_NAME)

    # Current holders
    guild = interaction.guild
    current_ids = data.get("current_holders", [])
    current_members = [guild.get_member(uid) for uid in current_ids]
    current_mentions = [m.mention for m in current_members if m is not None]

    # Upcoming
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

@schedule_group.command(name="show", description="Show current schedule")
async def schedule_show(interaction: discord.Interaction):
    cfg = get_data()["config"]
    if not cfg["schedule"].get("enabled", False):
        await interaction.response.send_message(t(interaction.guild_id, "schedule_show_disabled"), ephemeral=True)
        return
    desc = schedule_description(cfg)
    nxt = get_data().get("next_run_ts")
    when = human_dt(nxt) if nxt else "—"
    await interaction.response.send_message(t(interaction.guild_id, "schedule_show_enabled", desc=desc, when=when), ephemeral=True)

@schedule_group.command(name="stop", description="Disable automatic rotation")
async def schedule_stop(interaction: discord.Interaction):
    if not is_admin_or_permitted(interaction.user):
        await interaction.response.send_message(t(interaction.guild_id, "not_admin_or_permitted"), ephemeral=True)
        return
    get_data()["config"]["schedule"]["enabled"] = False
    get_data()["next_run_ts"] = None
    save_data()
    await interaction.response.send_message(t(interaction.guild_id, "schedule_stopped"), ephemeral=True)
    logger.debug("/schedule stop by %s", interaction.user)

@schedule_group.command(name="set", description="Set schedule (daily/weekly/monthly/every_n_days)")
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
    time: str = "09:00",
    n_days: Optional[app_commands.Range[int, 1, 60]] = None,
    weekday: Optional[app_commands.Range[int, 0, 6]] = None,
    day_of_month: Optional[app_commands.Range[int, 1, 31]] = None
):
    if not is_admin_or_permitted(interaction.user):
        await interaction.response.send_message(t(interaction.guild_id, "not_admin_or_permitted"), ephemeral=True)
        return
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
    # compute next run and save
    await save_and_reschedule()
    desc = schedule_description(get_data()["config"])
    nxt = get_data().get("next_run_ts")
    when = human_dt(nxt) if nxt else "—"
    await interaction.response.send_message(t(interaction.guild_id, "schedule_set", desc=desc, when=when), ephemeral=True)
    logger.debug("/schedule set %s by %s", s, interaction.user)

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
