from fastapi import FastAPI, Request, Form, UploadFile, File, Query
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from pathlib import Path
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
import shutil
import tempfile
import asyncio
from io import BytesIO
import hashlib
import hmac
import secrets
import re

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key=os.getenv("SESSION_SECRET", "super-secret-key-change-this"))

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

RENDER_DISK_PATH = os.getenv("RENDER_DISK_PATH", "")
if RENDER_DISK_PATH:
    DB_PATH = os.path.join(RENDER_DISK_PATH, "database.db")
else:
    DB_PATH = str(BASE_DIR / "database.db")

GUILD_DATA_TABLES = [
    "members",
    "pending_members",
    "former_members",
    "name_history",
    "guild_stat_snapshots",
    "guild_stat_snapshot_rows",
    "kill_reports",
    "kill_report_rows",
    "guild_fest_reports",
    "guild_fest_report_rows",
    "guild_settings",
]


SITE_ADMIN_PASSWORD = os.getenv("SITE_ADMIN_PASSWORD", "siteadmin123")
DEFAULT_MJ_GUILD_PASSWORD = os.getenv("DEFAULT_MJ_GUILD_PASSWORD", "admin123")
DEFAULT_MJ_ADMIN_PASSWORD = os.getenv("DEFAULT_MJ_ADMIN_PASSWORD", "admin123")


def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 120_000).hex()
    return f"pbkdf2_sha256${salt}${digest}"


def verify_password(password: str, stored_hash: str) -> bool:
    if not stored_hash or "$" not in stored_hash:
        return False
    try:
        method, salt, digest = stored_hash.split("$", 2)
        if method != "pbkdf2_sha256":
            return False
        check = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 120_000).hex()
        return hmac.compare_digest(check, digest)
    except Exception:
        return False


def valid_guild_tag(guild_tag: str) -> bool:
    return bool(guild_tag) and len(guild_tag) == 3 and all(ch.isprintable() and not ch.isspace() for ch in guild_tag)


def current_guild_id(request: Request):
    return request.session.get("guild_id")


def current_guild_tag(request: Request):
    return request.session.get("guild_tag", "")


def require_guild(request: Request):
    gid = current_guild_id(request)
    if not gid:
        return None
    return int(gid)


def is_site_admin(request: Request):
    return request.session.get("site_admin", False)



def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def column_exists(conn, table_name, column_name):
    c = conn.cursor()
    c.execute(f"PRAGMA table_info({table_name})")
    return column_name in [row[1] for row in c.fetchall()]


def init_db():
    conn = get_conn()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS guilds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_tag TEXT UNIQUE COLLATE BINARY,
            email TEXT,
            guild_password_hash TEXT,
            admin_password_hash TEXT,
            guild_password_plain TEXT DEFAULT '',
            admin_password_plain TEXT DEFAULT '',
            is_disabled INTEGER DEFAULT 0,
            disabled_reason TEXT DEFAULT '',
            created_at TEXT,
            updated_at TEXT
        )
    """)

    for col, definition in {
        "guild_password_plain": "TEXT DEFAULT ''",
        "admin_password_plain": "TEXT DEFAULT ''",
        "is_disabled": "INTEGER DEFAULT 0",
        "disabled_reason": "TEXT DEFAULT ''",
    }.items():
        if not column_exists(conn, "guilds", col):
            c.execute(f"ALTER TABLE guilds ADD COLUMN {col} {definition}")

    c.execute("""
        CREATE TABLE IF NOT EXISTS members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            igg_id TEXT UNIQUE,
            name TEXT,
            rank TEXT,
            might INTEGER,
            kills INTEGER,
            edm INTEGER,
            mana INTEGER DEFAULT 0,
            sigils INTEGER DEFAULT 0,
            kingdom_limit INTEGER DEFAULT 0,
            comments TEXT,
            alt_account INTEGER DEFAULT 0,
            troop_comp TEXT DEFAULT 'N/A',
            communication_method TEXT DEFAULT 'N/A',
            whatsapp_number TEXT DEFAULT '',
            discord_username TEXT DEFAULT '',
            watchlist_flag INTEGER DEFAULT 0,
            created_at TEXT,
            updated_at TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS pending_members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            igg_id TEXT UNIQUE,
            name TEXT,
            rank TEXT,
            might INTEGER,
            kills INTEGER,
            edm INTEGER,
            source_filename TEXT,
            imported_at TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS former_members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            igg_id TEXT,
            name TEXT,
            rank TEXT,
            might INTEGER,
            kills INTEGER,
            edm INTEGER,
            mana INTEGER DEFAULT 0,
            sigils INTEGER DEFAULT 0,
            kingdom_limit INTEGER DEFAULT 0,
            comments TEXT,
            alt_account INTEGER DEFAULT 0,
            troop_comp TEXT DEFAULT 'N/A',
            communication_method TEXT DEFAULT 'N/A',
            whatsapp_number TEXT DEFAULT '',
            discord_username TEXT DEFAULT '',
            watchlist_flag INTEGER DEFAULT 0,
            removal_reason TEXT,
            removal_notes TEXT,
            removed_at TEXT,
            original_created_at TEXT,
            original_updated_at TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS name_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            igg_id TEXT,
            old_name TEXT,
            new_name TEXT,
            changed_at TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS guild_stat_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_name TEXT,
            imported_at TEXT,
            source_filename TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS guild_stat_snapshot_rows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id INTEGER,
            igg_id TEXT,
            player_name TEXT,
            rank TEXT,
            might INTEGER,
            kills INTEGER,
            edm INTEGER
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS kill_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_name TEXT,
            generated_at TEXT,
            start_snapshot_id INTEGER,
            end_snapshot_id INTEGER,
            target_kill_increase INTEGER,
            target_edm_increase INTEGER,
            target_edm_per_kill INTEGER,
            avg_kill_increase REAL,
            avg_edm_increase REAL,
            avg_edm_per_kill REAL
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS kill_report_rows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id INTEGER,
            igg_id TEXT,
            player_name TEXT,
            kill_increase INTEGER,
            edm_increase INTEGER,
            edm_per_kill INTEGER,
            pass_kills INTEGER,
            pass_edm INTEGER,
            pass_edm_per_kill INTEGER,
            overall_pass INTEGER
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS guild_fest_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_name TEXT,
            generated_at TEXT,
            source_filename TEXT,
            pass_score INTEGER,
            avg_score REAL
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS guild_fest_report_rows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id INTEGER,
            player_name TEXT,
            guild_fest_score INTEGER,
            completed INTEGER,
            total INTEGER,
            completed_bonus TEXT,
            passed INTEGER
        )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS guild_settings (
        setting_key TEXT PRIMARY KEY,
        setting_value TEXT
        )
    """)


    c.execute("""
        CREATE TABLE IF NOT EXISTS guild_settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER,
            setting_key TEXT,
            setting_value TEXT,
            UNIQUE(guild_id, setting_key)
        )
    """)

    # Option B multiguild migration: add guild_id to every guild-owned table.
    for table in GUILD_DATA_TABLES:
        if not column_exists(conn, table, "guild_id"):
            c.execute(f"ALTER TABLE {table} ADD COLUMN guild_id INTEGER")

    for col, definition in {
        "mana": "INTEGER DEFAULT 0",
        "sigils": "INTEGER DEFAULT 0",
        "kingdom_limit": "INTEGER DEFAULT 0",
        "comments": "TEXT",
        "alt_account": "INTEGER DEFAULT 0",
        "troop_comp": "TEXT DEFAULT 'N/A'",
        "communication_method": "TEXT DEFAULT 'N/A'",
        "whatsapp_number": "TEXT DEFAULT ''",
        "discord_username": "TEXT DEFAULT ''",
        "watchlist_flag": "INTEGER DEFAULT 0",
        "created_at": "TEXT",
        "updated_at": "TEXT",
    }.items():
        if not column_exists(conn, "members", col):
            c.execute(f"ALTER TABLE members ADD COLUMN {col} {definition}")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    c.execute("UPDATE members SET rank = 'RANK1' WHERE rank IS NULL OR TRIM(rank) = ''")
    c.execute("UPDATE members SET rank = 'RANK1' WHERE rank = 'R1'")
    c.execute("UPDATE members SET rank = 'RANK2' WHERE rank = 'R2'")
    c.execute("UPDATE members SET rank = 'RANK3' WHERE rank = 'R3'")
    c.execute("UPDATE members SET rank = 'RANK4' WHERE rank = 'R4'")
    c.execute("UPDATE members SET rank = 'RANK5' WHERE rank = 'R5'")
    c.execute("UPDATE members SET mana = COALESCE(mana, 0)")
    c.execute("UPDATE members SET sigils = COALESCE(sigils, 0)")
    c.execute("UPDATE members SET kingdom_limit = COALESCE(kingdom_limit, 0)")
    c.execute("UPDATE members SET comments = COALESCE(comments, '')")
    c.execute("UPDATE members SET alt_account = COALESCE(alt_account, 0)")
    c.execute("UPDATE members SET troop_comp = COALESCE(troop_comp, 'N/A') WHERE troop_comp IS NULL OR TRIM(troop_comp) = ''")
    c.execute("UPDATE members SET communication_method = COALESCE(communication_method, 'N/A') WHERE communication_method IS NULL OR TRIM(communication_method) = ''")
    c.execute("UPDATE members SET whatsapp_number = COALESCE(whatsapp_number, '')")
    c.execute("UPDATE members SET discord_username = COALESCE(discord_username, '')")
    c.execute("UPDATE members SET watchlist_flag = COALESCE(watchlist_flag, 0)")
    c.execute("UPDATE members SET created_at = COALESCE(created_at, ?) WHERE created_at IS NULL OR TRIM(created_at) = ''", (now,))
    c.execute("UPDATE members SET updated_at = COALESCE(updated_at, ?) WHERE updated_at IS NULL OR TRIM(updated_at) = ''", (now,))

    # Create the existing guild as M/J and attach all existing data to it.
    c.execute("SELECT id FROM guilds WHERE guild_tag = ? COLLATE BINARY", ("M/J",))
    row = c.fetchone()
    if row:
        default_guild_id = row["id"]
    else:
        c.execute("""
            INSERT INTO guilds (guild_tag, email, guild_password_hash, admin_password_hash, guild_password_plain, admin_password_plain, is_disabled, disabled_reason, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "M/J",
            os.getenv("DEFAULT_MJ_EMAIL", "owner@example.com"),
            hash_password(DEFAULT_MJ_GUILD_PASSWORD),
            hash_password(DEFAULT_MJ_ADMIN_PASSWORD),
            DEFAULT_MJ_GUILD_PASSWORD,
            DEFAULT_MJ_ADMIN_PASSWORD,
            0,
            "",
            now,
            now
        ))
        default_guild_id = c.lastrowid

    for table in GUILD_DATA_TABLES:
        if column_exists(conn, table, "guild_id"):
            c.execute(f"UPDATE {table} SET guild_id = ? WHERE guild_id IS NULL", (default_guild_id,))


    c.execute("""
        UPDATE guilds
        SET guild_password_plain = CASE WHEN guild_password_plain IS NULL OR TRIM(guild_password_plain) = '' THEN ? ELSE guild_password_plain END,
            admin_password_plain = CASE WHEN admin_password_plain IS NULL OR TRIM(admin_password_plain) = '' THEN ? ELSE admin_password_plain END,
            is_disabled = COALESCE(is_disabled, 0),
            disabled_reason = COALESCE(disabled_reason, '')
        WHERE guild_tag = ? COLLATE BINARY
    """, (DEFAULT_MJ_GUILD_PASSWORD, DEFAULT_MJ_ADMIN_PASSWORD, "M/J"))

    conn.commit()
    conn.close()


init_db()


def is_admin(request: Request):
    return request.session.get("is_admin", False) and current_guild_id(request) is not None


def normalise_comm_fields(method, whatsapp_number, discord_username):
    method = (method or "N/A").strip()
    whatsapp_number = (whatsapp_number or "").strip()
    discord_username = (discord_username or "").strip()

    if method == "WhatsApp":
        discord_username = ""
    elif method == "Discord":
        whatsapp_number = ""
    elif method == "Both":
        pass
    else:
        method = "N/A"
        whatsapp_number = ""
        discord_username = ""

    return method, whatsapp_number, discord_username


def log_name_change(conn, guild_id, igg_id, old, new):
    if old and new and old != new:
        conn.execute("""
            INSERT INTO name_history (guild_id, igg_id, old_name, new_name, changed_at)
            VALUES (?, ?, ?, ?, ?)
        """, (guild_id, igg_id, old, new, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))


def create_current_roster_snapshot(guild_id: int, snapshot_name=None, source_filename="Auto roster snapshot"):
    conn = get_conn()
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if snapshot_name is None:
        snapshot_name = f"Manual Roster Snapshot {now}"

    c.execute("SELECT id FROM guild_stat_snapshots WHERE snapshot_name = ? AND guild_id = ?", (snapshot_name, guild_id))
    existing = c.fetchone()
    if existing:
        conn.close()
        return existing["id"]

    c.execute("""
        INSERT INTO guild_stat_snapshots (guild_id, snapshot_name, imported_at, source_filename)
        VALUES (?, ?, ?, ?)
    """, (guild_id, snapshot_name, now, source_filename))
    snapshot_id = c.lastrowid

    c.execute("SELECT * FROM members WHERE guild_id = ? ORDER BY LOWER(name)", (guild_id,))
    members = c.fetchall()

    for member in members:
        c.execute("""
            INSERT INTO guild_stat_snapshot_rows
            (guild_id, snapshot_id, igg_id, player_name, rank, might, kills, edm)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            snapshot_id,
            member["igg_id"],
            member["name"],
            member["rank"],
            member["might"] or 0,
            member["kills"] or 0,
            member["edm"] or 0,
        ))

    conn.commit()
    conn.close()
    return snapshot_id


async def weekly_auto_snapshot_loop():
    tz = ZoneInfo("Europe/London")
    while True:
        now = datetime.now(tz)
        days_until_sunday = (6 - now.weekday()) % 7
        target = (now + timedelta(days=days_until_sunday)).replace(hour=23, minute=0, second=0, microsecond=0)

        if target <= now:
            target += timedelta(days=7)

        await asyncio.sleep((target - now).total_seconds())

        run_date = datetime.now(tz).strftime("%Y-%m-%d")
        conn = get_conn()
        guilds = conn.execute("SELECT id FROM guilds").fetchall()
        conn.close()
        for guild in guilds:
            create_current_roster_snapshot(
                guild["id"],
                snapshot_name=f"Weekly Auto Snapshot {run_date}",
                source_filename="Automatic Sunday 23:00 roster snapshot"
            )

        await asyncio.sleep(60)


@app.on_event("startup")
async def start_auto_snapshot_task():
    asyncio.create_task(weekly_auto_snapshot_loop())


def get_sort_sql(sort_by, sort_dir):
    rank_sort = """
        CASE UPPER(COALESCE(rank, 'RANK1'))
            WHEN 'RANK1' THEN 1
            WHEN 'RANK2' THEN 2
            WHEN 'RANK3' THEN 3
            WHEN 'RANK4' THEN 4
            WHEN 'RANK5' THEN 5
            ELSE 0
        END
    """

    sort_map = {
        "name": "LOWER(COALESCE(name, ''))",
        "user_id": "LOWER(COALESCE(igg_id, ''))",
        "igg_id": "LOWER(COALESCE(igg_id, ''))",
        "might": "COALESCE(might, 0)",
        "kills": "COALESCE(kills, 0)",
        "rank": rank_sort,
        "edm": "COALESCE(edm, 0)",
        "mana": "COALESCE(mana, 0)",
        "sigils": "COALESCE(sigils, 0)",
        "kingdom": "COALESCE(kingdom_limit, 0)"
    }

    sort_column = sort_map.get(sort_by, "COALESCE(might, 0)")
    direction = "ASC" if sort_dir == "asc" else "DESC"

    return f"{sort_column} {direction}, LOWER(COALESCE(name, '')) ASC"


def build_members_query(guild_id, search="", rank_filter="", alt_filter="", troop_comp_filter="", communication_filter="", min_mana="", min_sigils="", watchlist_only="", sort_by="might", sort_dir="desc", include_user_id_search=False):
    sql = """
        SELECT m.*,
        (SELECT MAX(changed_at) FROM name_history nh WHERE nh.igg_id = m.igg_id AND nh.guild_id = m.guild_id) AS last_name_change
        FROM members m
        WHERE m.guild_id = ?
    """
    params = [guild_id]

    search = (search or "").strip()
    if search:
        like_value = f"%{search.lower()}%"
        if include_user_id_search:
            sql += """
                AND (
                    LOWER(COALESCE(m.name, '')) LIKE ?
                    OR LOWER(COALESCE(m.igg_id, '')) LIKE ?
                    OR LOWER(COALESCE(m.rank, '')) LIKE ?
                    OR CAST(COALESCE(m.might, 0) AS TEXT) LIKE ?
                    OR CAST(COALESCE(m.kills, 0) AS TEXT) LIKE ?
                    OR CAST(COALESCE(m.edm, 0) AS TEXT) LIKE ?
                    OR CAST(COALESCE(m.kingdom_limit, 0) AS TEXT) LIKE ?
                )
            """
            params.extend([like_value] * 7)
        else:
            sql += """
                AND (
                    LOWER(COALESCE(m.name, '')) LIKE ?
                    OR LOWER(COALESCE(m.rank, '')) LIKE ?
                    OR CAST(COALESCE(m.might, 0) AS TEXT) LIKE ?
                    OR CAST(COALESCE(m.kills, 0) AS TEXT) LIKE ?
                    OR CAST(COALESCE(m.edm, 0) AS TEXT) LIKE ?
                    OR CAST(COALESCE(m.kingdom_limit, 0) AS TEXT) LIKE ?
                )
            """
            params.extend([like_value] * 6)

    if rank_filter:
        sql += " AND UPPER(COALESCE(m.rank, '')) = ?"
        params.append(rank_filter.upper())

    if alt_filter == "yes":
        sql += " AND COALESCE(m.alt_account, 0) = 1"
    elif alt_filter == "no":
        sql += " AND COALESCE(m.alt_account, 0) = 0"

    if troop_comp_filter:
        sql += " AND COALESCE(m.troop_comp, 'N/A') = ?"
        params.append(troop_comp_filter)

    if communication_filter:
        sql += " AND COALESCE(m.communication_method, 'N/A') = ?"
        params.append(communication_filter)

    if min_mana != "":
        try:
            sql += " AND COALESCE(m.mana, 0) >= ?"
            params.append(int(min_mana))
        except ValueError:
            pass

    if min_sigils != "":
        try:
            sql += " AND COALESCE(m.sigils, 0) >= ?"
            params.append(int(min_sigils))
        except ValueError:
            pass

    if watchlist_only == "yes":
        sql += " AND COALESCE(m.watchlist_flag, 0) = 1"

    sql += f" ORDER BY {get_sort_sql(sort_by, sort_dir)}"
    return sql, params


def get_member_fail_stats(conn, guild_id, igg_id, member_name):
    c = conn.cursor()

    c.execute("""
        SELECT COUNT(*) AS total_count,
        SUM(CASE WHEN overall_pass = 0 THEN 1 ELSE 0 END) AS fail_count
        FROM kill_report_rows
        WHERE guild_id = ? AND igg_id = ?
    """, (guild_id, igg_id))
    kill_stats = c.fetchone()

    c.execute("""
        SELECT COUNT(*) AS total_count,
        SUM(CASE WHEN passed = 0 THEN 1 ELSE 0 END) AS fail_count
        FROM guild_fest_report_rows
        WHERE guild_id = ? AND LOWER(player_name) = LOWER(?)
    """, (guild_id, member_name))
    gf_stats = c.fetchone()

    kill_total = kill_stats["total_count"] or 0
    kill_fail = kill_stats["fail_count"] or 0
    gf_total = gf_stats["total_count"] or 0
    gf_fail = gf_stats["fail_count"] or 0

    return {
        "kill_total": kill_total,
        "kill_fail": kill_fail,
        "kill_consistent_fail": kill_fail >= 2,
        "gf_total": gf_total,
        "gf_fail": gf_fail,
        "gf_consistent_fail": gf_fail >= 2
    }


def get_watchlist_recommendations(conn, guild_id):
    c = conn.cursor()
    c.execute("SELECT igg_id, name, watchlist_flag FROM members WHERE guild_id = ? ORDER BY LOWER(name)", (guild_id,))
    members = c.fetchall()

    recommendations = []
    for member in members:
        if int(member["watchlist_flag"] or 0) == 1:
            continue
        stats = get_member_fail_stats(conn, guild_id, member["igg_id"], member["name"])
        if stats["kill_consistent_fail"] or stats["gf_consistent_fail"]:
            recommendations.append({
                "igg_id": member["igg_id"],
                "name": member["name"],
                **stats
            })

    return recommendations


def get_guild_settings(conn, guild_id):
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS guild_settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER,
            setting_key TEXT,
            setting_value TEXT,
            UNIQUE(guild_id, setting_key)
        )
    """)
    conn.commit()

    def get_value(key, default):
        c.execute("SELECT setting_value FROM guild_settings WHERE guild_id = ? AND setting_key = ?", (guild_id, key))
        row = c.fetchone()
        try:
            return int(row["setting_value"]) if row else default
        except (TypeError, ValueError):
            return default

    return {
        "min_mana": get_value("min_mana", 1),
        "min_sigils": get_value("min_sigils", 80),
        "report_fail_threshold": get_value("report_fail_threshold", 2),
        "auto_watch_requirements": get_value("auto_watch_requirements", 1)
    }


def get_dashboard_insights(conn, guild_id):
    c = conn.cursor()

    c.execute("SELECT COUNT(*) AS cnt FROM members WHERE guild_id = ? AND COALESCE(watchlist_flag, 0) = 1", (guild_id,))
    watchlist_count = c.fetchone()["cnt"] or 0

    c.execute("""
        SELECT COUNT(DISTINCT igg_id) AS cnt
        FROM kill_report_rows
        WHERE guild_id = ?
        AND report_id = (SELECT id FROM kill_reports WHERE guild_id = ? ORDER BY generated_at DESC LIMIT 1)
        AND overall_pass = 0
    """, (guild_id, guild_id))
    latest_kill_fail_count = c.fetchone()["cnt"] or 0

    c.execute("""
        SELECT COUNT(*) AS cnt
        FROM guild_fest_report_rows
        WHERE guild_id = ?
        AND report_id = (SELECT id FROM guild_fest_reports WHERE guild_id = ? ORDER BY generated_at DESC LIMIT 1)
        AND passed = 0
    """, (guild_id, guild_id))
    latest_gf_fail_count = c.fetchone()["cnt"] or 0

    settings = get_guild_settings(conn, guild_id)

    c.execute("SELECT COUNT(*) AS cnt FROM members WHERE guild_id = ? AND COALESCE(mana, 0) < ?", (guild_id, settings["min_mana"]))
    low_mana_count = c.fetchone()["cnt"] or 0

    c.execute("SELECT COUNT(*) AS cnt FROM members WHERE guild_id = ? AND COALESCE(sigils, 0) < ?", (guild_id, settings["min_sigils"]))
    low_sigils_count = c.fetchone()["cnt"] or 0

    c.execute("SELECT COUNT(*) AS cnt FROM pending_members WHERE guild_id = ?", (guild_id,))
    pending_count = c.fetchone()["cnt"] or 0

    return {
        "watchlist_count": watchlist_count,
        "latest_kill_fail_count": latest_kill_fail_count,
        "latest_gf_fail_count": latest_gf_fail_count,
        "low_mana_count": low_mana_count,
        "low_sigils_count": low_sigils_count,
        "pending_count": pending_count
    }


def get_pending_comparison(conn, guild_id):
    c = conn.cursor()
    c.execute("""
        SELECT
            AVG(COALESCE(might, 0)) AS avg_might,
            AVG(COALESCE(kills, 0)) AS avg_kills,
            AVG(COALESCE(edm, 0)) AS avg_edm
        FROM members
        WHERE guild_id = ?
    """, (guild_id,))
    averages = c.fetchone()
    avg_might = averages["avg_might"] or 0
    avg_kills = averages["avg_kills"] or 0
    avg_edm = averages["avg_edm"] or 0

    c.execute("SELECT * FROM pending_members WHERE guild_id = ? ORDER BY imported_at DESC, LOWER(name) ASC", (guild_id,))
    pending_members = c.fetchall()

    rows = []
    for member in pending_members:
        might = member["might"] or 0
        kills = member["kills"] or 0
        edm = member["edm"] or 0
        rows.append({
            "member": member,
            "might_diff": might - avg_might,
            "kills_diff": kills - avg_kills,
            "edm_diff": edm - avg_edm,
            "might_above": might >= avg_might if avg_might else None,
            "kills_above": kills >= avg_kills if avg_kills else None,
            "edm_above": edm >= avg_edm if avg_edm else None,
        })

    return {
        "avg_might": avg_might,
        "avg_kills": avg_kills,
        "avg_edm": avg_edm,
        "rows": rows,
    }



@app.get("/", response_class=HTMLResponse)
def landing_or_dashboard(
    request: Request,
    search: str = Query(default=""),
    sort_by: str = Query(default="might"),
    sort_dir: str = Query(default="desc"),
    rank_filter: str = Query(default=""),
    alt_filter: str = Query(default=""),
    troop_comp_filter: str = Query(default=""),
    min_mana: str = Query(default=""),
    min_sigils: str = Query(default=""),
    watchlist_only: str = Query(default="")
):
    if not current_guild_id(request):
        return templates.TemplateResponse(request, "landing.html", {})
    return dashboard_view(request, search, sort_by, sort_dir, rank_filter, alt_filter, troop_comp_filter, min_mana, min_sigils, watchlist_only)


@app.get("/guild/login", response_class=HTMLResponse)
def guild_login_page(request: Request):
    return templates.TemplateResponse(request, "guild_login.html", {"error": ""})


@app.post("/guild/login")
def guild_login(request: Request, guild_tag: str = Form(...), guild_password: str = Form(...)):
    guild_tag = guild_tag.strip()
    conn = get_conn()
    guild = conn.execute("SELECT * FROM guilds WHERE guild_tag = ? COLLATE BINARY", (guild_tag,)).fetchone()
    conn.close()
    if not guild or not verify_password(guild_password, guild["guild_password_hash"]):
        return templates.TemplateResponse(request, "guild_login.html", {"error": "Incorrect guild tag or password."}, status_code=401)
    if int(guild["is_disabled"] or 0) == 1:
        reason = (guild["disabled_reason"] or "This guild has been disabled by site admin.").strip()
        return templates.TemplateResponse(request, "guild_login.html", {"error": f"Guild is disabled. {reason}"}, status_code=403)
    request.session.clear()
    request.session["guild_id"] = guild["id"]
    request.session["guild_tag"] = guild["guild_tag"]
    request.session["is_admin"] = False
    return RedirectResponse(url="/", status_code=302)


@app.get("/guild/create", response_class=HTMLResponse)
def create_guild_page(request: Request):
    return templates.TemplateResponse(request, "create_guild.html", {"error": ""})


@app.post("/guild/create")
def create_guild(
    request: Request,
    guild_tag: str = Form(...),
    email: str = Form(...),
    confirm_email: str = Form(...),
    guild_password: str = Form(...),
    admin_password: str = Form(...)
):
    guild_tag = guild_tag.strip()
    email = email.strip()
    confirm_email = confirm_email.strip()
    if not valid_guild_tag(guild_tag):
        return templates.TemplateResponse(request, "create_guild.html", {"error": "Guild tag must be exactly 3 printable, non-space characters."}, status_code=400)
    if email.lower() != confirm_email.lower():
        return templates.TemplateResponse(request, "create_guild.html", {"error": "Email addresses do not match."}, status_code=400)
    if not guild_password or not admin_password:
        return templates.TemplateResponse(request, "create_guild.html", {"error": "Guild and admin passwords are required."}, status_code=400)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            INSERT INTO guilds (guild_tag, email, guild_password_hash, admin_password_hash, guild_password_plain, admin_password_plain, is_disabled, disabled_reason, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (guild_tag, email, hash_password(guild_password), hash_password(admin_password), guild_password, admin_password, 0, "", now, now))
        guild_id = c.lastrowid
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return templates.TemplateResponse(request, "create_guild.html", {"error": "That guild tag already exists."}, status_code=400)
    conn.close()
    request.session.clear()
    request.session["guild_id"] = guild_id
    request.session["guild_tag"] = guild_tag
    request.session["is_admin"] = True
    return RedirectResponse(url="/", status_code=302)


@app.get("/guild/logout")
def guild_logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/", status_code=302)


@app.get("/site-admin/login", response_class=HTMLResponse)
def site_admin_login_page(request: Request):
    return templates.TemplateResponse(request, "site_admin_login.html", {"error": ""})


@app.post("/site-admin/login")
def site_admin_login(request: Request, password: str = Form(...)):
    if password != SITE_ADMIN_PASSWORD:
        return templates.TemplateResponse(request, "site_admin_login.html", {"error": "Incorrect site admin password."}, status_code=401)
    request.session.clear()
    request.session["site_admin"] = True
    return RedirectResponse(url="/site-admin", status_code=302)


@app.get("/site-admin", response_class=HTMLResponse)
def site_admin_dashboard(request: Request):
    if not is_site_admin(request):
        return RedirectResponse(url="/site-admin/login", status_code=302)
    conn = get_conn()
    rows = conn.execute("""
        SELECT g.*, COUNT(m.id) AS member_count
        FROM guilds g
        LEFT JOIN members m ON m.guild_id = g.id
        GROUP BY g.id
        ORDER BY g.created_at DESC
    """).fetchall()
    conn.close()
    return templates.TemplateResponse(request, "site_admin_dashboard.html", {"guilds": rows})


@app.get("/site-admin/guild/{guild_id}", response_class=HTMLResponse)
def site_admin_edit_guild_page(request: Request, guild_id: int):
    if not is_site_admin(request):
        return RedirectResponse(url="/site-admin/login", status_code=302)
    conn = get_conn()
    guild = conn.execute("SELECT * FROM guilds WHERE id = ?", (guild_id,)).fetchone()
    conn.close()
    if not guild:
        return HTMLResponse("<h2>Guild not found</h2>", status_code=404)
    return templates.TemplateResponse(request, "site_admin_edit_guild.html", {"guild": guild})


@app.post("/site-admin/guild/{guild_id}")
def site_admin_edit_guild(
    request: Request,
    guild_id: int,
    email: str = Form(""),
    guild_password: str = Form(""),
    admin_password: str = Form(""),
    disabled_reason: str = Form("")
):
    if not is_site_admin(request):
        return RedirectResponse(url="/site-admin/login", status_code=302)
    conn = get_conn()
    c = conn.cursor()
    guild = c.execute("SELECT * FROM guilds WHERE id = ?", (guild_id,)).fetchone()
    if not guild:
        conn.close()
        return HTMLResponse("<h2>Guild not found</h2>", status_code=404)
    updates = ["email = ?", "disabled_reason = ?", "updated_at = ?"]
    values = [email.strip(), disabled_reason.strip(), datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
    if guild_password.strip():
        updates.append("guild_password_hash = ?")
        values.append(hash_password(guild_password.strip()))
        updates.append("guild_password_plain = ?")
        values.append(guild_password.strip())
    if admin_password.strip():
        updates.append("admin_password_hash = ?")
        values.append(hash_password(admin_password.strip()))
        updates.append("admin_password_plain = ?")
        values.append(admin_password.strip())
    values.append(guild_id)
    c.execute(f"UPDATE guilds SET {', '.join(updates)} WHERE id = ?", values)
    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/site-admin/guild/{guild_id}", status_code=302)




@app.post("/site-admin/guild/{guild_id}/disable")
def site_admin_disable_guild(request: Request, guild_id: int, disabled_reason: str = Form("Disabled by site admin")):
    if not is_site_admin(request):
        return RedirectResponse(url="/site-admin/login", status_code=302)
    conn = get_conn()
    conn.execute(
        "UPDATE guilds SET is_disabled = 1, disabled_reason = ?, updated_at = ? WHERE id = ?",
        (disabled_reason.strip() or "Disabled by site admin", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), guild_id)
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/site-admin/guild/{guild_id}", status_code=302)


@app.post("/site-admin/guild/{guild_id}/enable")
def site_admin_enable_guild(request: Request, guild_id: int):
    if not is_site_admin(request):
        return RedirectResponse(url="/site-admin/login", status_code=302)
    conn = get_conn()
    conn.execute(
        "UPDATE guilds SET is_disabled = 0, disabled_reason = '', updated_at = ? WHERE id = ?",
        (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), guild_id)
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/site-admin/guild/{guild_id}", status_code=302)


@app.post("/site-admin/guild/{guild_id}/delete")
def site_admin_delete_guild(request: Request, guild_id: int, confirm_text: str = Form(...)):
    if not is_site_admin(request):
        return RedirectResponse(url="/site-admin/login", status_code=302)
    conn = get_conn()
    c = conn.cursor()
    guild = c.execute("SELECT * FROM guilds WHERE id = ?", (guild_id,)).fetchone()
    if not guild:
        conn.close()
        return HTMLResponse("<h2>Guild not found</h2>", status_code=404)
    if confirm_text != guild["guild_tag"]:
        conn.close()
        return HTMLResponse("<h2>Confirmation text did not match guild tag. Guild was not deleted.</h2>", status_code=400)
    for table in reversed(GUILD_DATA_TABLES):
        if column_exists(conn, table, "guild_id"):
            c.execute(f"DELETE FROM {table} WHERE guild_id = ?", (guild_id,))
    c.execute("DELETE FROM guilds WHERE id = ?", (guild_id,))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/site-admin", status_code=302)


@app.get("/site-admin/impersonate/{guild_id}")
def site_admin_impersonate(request: Request, guild_id: int):
    if not is_site_admin(request):
        return RedirectResponse(url="/site-admin/login", status_code=302)
    conn = get_conn()
    guild = conn.execute("SELECT * FROM guilds WHERE id = ?", (guild_id,)).fetchone()
    conn.close()
    if not guild:
        return HTMLResponse("<h2>Guild not found</h2>", status_code=404)
    request.session.clear()
    request.session["guild_id"] = guild["id"]
    request.session["guild_tag"] = guild["guild_tag"]
    request.session["is_admin"] = True
    return RedirectResponse(url="/", status_code=302)


@app.get("/site-admin/logout")
def site_admin_logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/", status_code=302)

def dashboard_view(
    request: Request,
    search: str = Query(default=""),
    sort_by: str = Query(default="might"),
    sort_dir: str = Query(default="desc"),
    rank_filter: str = Query(default=""),
    alt_filter: str = Query(default=""),
    troop_comp_filter: str = Query(default=""),
    communication_filter: str = Query(default=""),
    min_mana: str = Query(default=""),
    min_sigils: str = Query(default=""),
    watchlist_only: str = Query(default="")
):
    guild_id = require_guild(request)
    if not guild_id:
        return RedirectResponse(url="/", status_code=302)

    conn = get_conn()
    c = conn.cursor()

    admin_view = is_admin(request)
    sql, params = build_members_query(
        guild_id, search, rank_filter, alt_filter, troop_comp_filter, communication_filter, min_mana, min_sigils,
        watchlist_only, sort_by, sort_dir, include_user_id_search=admin_view
    )
    c.execute(sql, params)
    members = c.fetchall()

    c.execute("SELECT MIN(kingdom_limit) AS guild_max_kingdom FROM members WHERE guild_id = ? AND COALESCE(kingdom_limit, 0) > 0", (guild_id,))
    guild_max_kingdom = c.fetchone()["guild_max_kingdom"] or 0

    c.execute("SELECT * FROM kill_reports WHERE guild_id = ? ORDER BY generated_at DESC LIMIT 1", (guild_id,))
    latest_kill_report = c.fetchone()

    c.execute("SELECT * FROM guild_fest_reports WHERE guild_id = ? ORDER BY generated_at DESC LIMIT 1", (guild_id,))
    latest_guild_fest_report = c.fetchone()

    watchlist_summary = []
    watchlist_recommendations = []
    dashboard_insights = {}

    if is_admin(request):
        c.execute("SELECT igg_id, name FROM members WHERE guild_id = ? AND COALESCE(watchlist_flag, 0) = 1 ORDER BY LOWER(name)", (guild_id,))
        for member in c.fetchall():
            stats = get_member_fail_stats(conn, guild_id, member["igg_id"], member["name"])
            watchlist_summary.append({"igg_id": member["igg_id"], "name": member["name"], **stats})

        watchlist_recommendations = get_watchlist_recommendations(conn, guild_id)
        dashboard_insights = get_dashboard_insights(conn, guild_id)

    conn.close()

    return templates.TemplateResponse(request, "dashboard.html", {
        "members": members,
        "search": search,
        "sort_by": sort_by,
        "sort_dir": sort_dir,
        "rank_filter": rank_filter,
        "alt_filter": alt_filter,
        "troop_comp_filter": troop_comp_filter,
        "communication_filter": communication_filter,
        "min_mana": min_mana,
        "min_sigils": min_sigils,
        "watchlist_only": watchlist_only,
        "latest_kill_report": latest_kill_report,
        "latest_guild_fest_report": latest_guild_fest_report,
        "watchlist_summary": watchlist_summary,
        "watchlist_recommendations": watchlist_recommendations,
        "dashboard_insights": dashboard_insights,
        "guild_max_kingdom": guild_max_kingdom,
        "guild_tag": current_guild_tag(request),
        "is_admin": admin_view
    })



def get_auto_watchlist_candidates(conn, guild_id):
    c = conn.cursor()
    settings = get_guild_settings(conn, guild_id)

    c.execute("""
        SELECT * FROM members
        WHERE guild_id = ?
        AND COALESCE(watchlist_flag, 0) = 0
        AND (
            COALESCE(mana, 0) < ?
            OR COALESCE(sigils, 0) < ?
        )
        ORDER BY LOWER(name)
    """, (guild_id, settings["min_mana"], settings["min_sigils"]))
    requirement_failures = c.fetchall()

    c.execute("""
        SELECT m.*, COUNT(krr.id) AS fail_count
        FROM members m
        JOIN kill_report_rows krr ON krr.igg_id = m.igg_id AND krr.guild_id = m.guild_id
        WHERE m.guild_id = ?
        AND COALESCE(m.watchlist_flag, 0) = 0
        AND krr.overall_pass = 0
        GROUP BY m.igg_id
        HAVING COUNT(krr.id) >= ?
        ORDER BY fail_count DESC, LOWER(m.name)
    """, (guild_id, settings["report_fail_threshold"]))
    kill_failures = c.fetchall()

    c.execute("""
        SELECT m.*, COUNT(gfrr.id) AS fail_count
        FROM members m
        JOIN guild_fest_report_rows gfrr ON LOWER(gfrr.player_name) = LOWER(m.name) AND gfrr.guild_id = m.guild_id
        WHERE m.guild_id = ?
        AND COALESCE(m.watchlist_flag, 0) = 0
        AND gfrr.passed = 0
        GROUP BY m.igg_id
        HAVING COUNT(gfrr.id) >= ?
        ORDER BY fail_count DESC, LOWER(m.name)
    """, (guild_id, settings["report_fail_threshold"]))
    guild_fest_failures = c.fetchall()

    return {
        "requirement_failures": requirement_failures,
        "kill_failures": kill_failures,
        "guild_fest_failures": guild_fest_failures
    }


@app.get("/guild-requirements", response_class=HTMLResponse)
def guild_requirements_page(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    guild_id = require_guild(request)
    conn = get_conn()
    c = conn.cursor()
    settings = get_guild_settings(conn, guild_id)

    c.execute("""
        SELECT * FROM members
        WHERE guild_id = ?
        AND COALESCE(mana, 0) < ?
        AND COALESCE(sigils, 0) >= ?
        ORDER BY COALESCE(mana, 0) ASC, LOWER(name) ASC
    """, (guild_id, settings["min_mana"], settings["min_sigils"]))
    low_mana_only = c.fetchall()

    c.execute("""
        SELECT * FROM members
        WHERE guild_id = ?
        AND COALESCE(sigils, 0) < ?
        AND COALESCE(mana, 0) >= ?
        ORDER BY COALESCE(sigils, 0) ASC, LOWER(name) ASC
    """, (guild_id, settings["min_sigils"], settings["min_mana"]))
    low_sigils_only = c.fetchall()

    c.execute("""
        SELECT * FROM members
        WHERE guild_id = ?
        AND COALESCE(mana, 0) < ?
        AND COALESCE(sigils, 0) < ?
        ORDER BY LOWER(name) ASC
    """, (guild_id, settings["min_mana"], settings["min_sigils"]))
    both = c.fetchall()

    low_mana = low_mana_only + both
    low_sigils = low_sigils_only + both

    auto_watchlist = get_auto_watchlist_candidates(conn, guild_id)

    conn.close()

    return templates.TemplateResponse(request, "guild_requirements.html", {
        "settings": settings,
        "low_mana": low_mana,
        "low_sigils": low_sigils,
        "low_mana_only": low_mana_only,
        "low_sigils_only": low_sigils_only,
        "both": both,
        "auto_watchlist": auto_watchlist,
        "is_admin": True
    })


@app.post("/guild-requirements/update")
def update_guild_requirements(
    request: Request,
    min_mana: int = Form(...),
    min_sigils: int = Form(...),
    report_fail_threshold: int = Form(2),
    auto_watch_requirements: str | None = Form(default=None)
):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    min_mana = max(0, int(min_mana))
    min_sigils = max(0, int(min_sigils))
    report_fail_threshold = max(1, int(report_fail_threshold))
    auto_watch_requirements_value = 1 if auto_watch_requirements else 0

    guild_id = require_guild(request)
    conn = get_conn()
    c = conn.cursor()

    c.execute("""
        INSERT OR REPLACE INTO guild_settings (guild_id, setting_key, setting_value)
        VALUES (?, ?, ?)
    """, (guild_id, "min_mana", str(min_mana)))

    c.execute("""
        INSERT OR REPLACE INTO guild_settings (guild_id, setting_key, setting_value)
        VALUES (?, ?, ?)
    """, (guild_id, "min_sigils", str(min_sigils)))

    c.execute("""
        INSERT OR REPLACE INTO guild_settings (guild_id, setting_key, setting_value)
        VALUES (?, ?, ?)
    """, (guild_id, "report_fail_threshold", str(report_fail_threshold)))

    c.execute("""
        INSERT OR REPLACE INTO guild_settings (guild_id, setting_key, setting_value)
        VALUES (?, ?, ?)
    """, (guild_id, "auto_watch_requirements", str(auto_watch_requirements_value)))

    conn.commit()
    conn.close()

    return RedirectResponse(url="/guild-requirements", status_code=302)

@app.get("/pending-members", response_class=HTMLResponse)
def pending_members_page(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    guild_id = require_guild(request)
    conn = get_conn()
    pending_comparison = get_pending_comparison(conn, guild_id)
    conn.close()

    return templates.TemplateResponse(request, "pending_members.html", {
        "pending_members": [row["member"] for row in pending_comparison["rows"]],
        "pending_comparison": pending_comparison,
        "is_admin": True
    })


@app.post("/pending-members/{pending_id}/approve")
def approve_pending_member(request: Request, pending_id: int):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    guild_id = require_guild(request)
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM pending_members WHERE id = ? AND guild_id = ?", (pending_id, guild_id))
    pending = c.fetchone()

    if not pending:
        conn.close()
        return HTMLResponse("<h2>Pending member not found</h2>", status_code=404)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    c.execute("""
        INSERT OR REPLACE INTO members
        (guild_id, igg_id, name, rank, might, kills, edm, mana, sigils, kingdom_limit, alt_account, troop_comp,
         communication_method, whatsapp_number, discord_username, watchlist_flag, comments, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        guild_id, pending["igg_id"], pending["name"], pending["rank"], pending["might"], pending["kills"], pending["edm"],
        0, 0, 0, 0, "N/A", "N/A", "", "", 0, "", now, now
    ))

    c.execute("DELETE FROM pending_members WHERE id = ? AND guild_id = ?", (pending_id, guild_id))
    conn.commit()
    conn.close()

    return RedirectResponse(url="/pending-members", status_code=302)


@app.post("/pending-members/{pending_id}/reject")
def reject_pending_member(request: Request, pending_id: int):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    guild_id = require_guild(request)
    conn = get_conn()
    conn.execute("DELETE FROM pending_members WHERE id = ? AND guild_id = ?", (pending_id, guild_id))
    conn.commit()
    conn.close()

    return RedirectResponse(url="/pending-members", status_code=302)


@app.get("/member/{igg_id}", response_class=HTMLResponse)
def member_page(request: Request, igg_id: str):
    guild_id = require_guild(request)
    if not guild_id:
        return RedirectResponse(url="/", status_code=302)
    conn = get_conn()
    c = conn.cursor()

    c.execute("SELECT * FROM members WHERE igg_id = ? AND guild_id = ?", (igg_id, guild_id))
    member = c.fetchone()

    if not member:
        conn.close()
        return HTMLResponse("<h2>Member not found</h2>", status_code=404)

    c.execute("SELECT * FROM name_history WHERE igg_id = ? AND guild_id = ? ORDER BY changed_at DESC", (igg_id, guild_id))
    history = c.fetchall()

    c.execute("""
        SELECT kr.generated_at, krr.player_name, krr.overall_pass
        FROM kill_report_rows krr
        JOIN kill_reports kr ON kr.id = krr.report_id
        WHERE krr.igg_id = ? AND krr.guild_id = ? AND kr.guild_id = ?
        ORDER BY kr.generated_at DESC
    """, (igg_id, guild_id, guild_id))
    kill_history = c.fetchall()

    c.execute("""
        SELECT gfr.generated_at, gfr.report_name, gfrr.passed
        FROM guild_fest_report_rows gfrr
        JOIN guild_fest_reports gfr ON gfr.id = gfrr.report_id
        WHERE LOWER(gfrr.player_name) = LOWER(?) AND gfrr.guild_id = ? AND gfr.guild_id = ?
        ORDER BY gfr.generated_at DESC
    """, (member["name"], guild_id, guild_id))
    guild_fest_history = c.fetchall()

    fail_stats = get_member_fail_stats(conn, guild_id, igg_id, member["name"])
    conn.close()

    return templates.TemplateResponse(request, "member.html", {
        "member": member,
        "history": history,
        "kill_history": kill_history,
        "guild_fest_history": guild_fest_history,
        "fail_stats": fail_stats,
        "is_admin": is_admin(request)
    })


@app.get("/member/{igg_id}/edit", response_class=HTMLResponse)
def edit_page(request: Request, igg_id: str):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    guild_id = require_guild(request)
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM members WHERE igg_id = ? AND guild_id = ?", (igg_id, guild_id))
    member = c.fetchone()
    conn.close()

    if not member:
        return HTMLResponse("<h2>Member not found</h2>", status_code=404)

    return templates.TemplateResponse(request, "edit_member.html", {
        "member": member,
        "is_admin": True
    })


@app.post("/member/{igg_id}/edit")
def edit_member(
    request: Request,
    igg_id: str,
    name: str = Form(...),
    rank: str = Form(...),
    might: int = Form(...),
    kills: int = Form(...),
    edm: int = Form(...),
    mana: int = Form(...),
    sigils: int = Form(...),
    kingdom_limit: int = Form(0),
    alt_account: str | None = Form(default=None),
    troop_comp: str = Form("N/A"),
    communication_method: str = Form("N/A"),
    whatsapp_number: str = Form(""),
    discord_username: str = Form(""),
    comments: str = Form("")
):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    mana = max(0, min(6, int(mana)))
    sigils = max(0, int(sigils))
    kingdom_limit = max(0, int(kingdom_limit))
    alt_account_value = 1 if alt_account else 0
    communication_method, whatsapp_number, discord_username = normalise_comm_fields(communication_method, whatsapp_number, discord_username)

    guild_id = require_guild(request)
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT name, watchlist_flag FROM members WHERE igg_id = ? AND guild_id = ?", (igg_id, guild_id))
    row = c.fetchone()

    if not row:
        conn.close()
        return HTMLResponse("<h2>Member not found</h2>", status_code=404)

    log_name_change(conn, guild_id, igg_id, row["name"], name)
    existing_watchlist = 0 if row["watchlist_flag"] is None else int(row["watchlist_flag"])

    c.execute("""
        UPDATE members
        SET name = ?, rank = ?, might = ?, kills = ?, edm = ?, mana = ?, sigils = ?, kingdom_limit = ?,
            alt_account = ?, troop_comp = ?, communication_method = ?, whatsapp_number = ?, discord_username = ?,
            comments = ?, watchlist_flag = ?, updated_at = ?
        WHERE igg_id = ? AND guild_id = ?
    """, (
        name, rank, might, kills, edm, mana, sigils, kingdom_limit,
        alt_account_value, troop_comp, communication_method, whatsapp_number, discord_username,
        comments, existing_watchlist, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), igg_id, guild_id
    ))

    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/member/{igg_id}", status_code=302)


@app.get("/member/{igg_id}/delete", response_class=HTMLResponse)
def confirm_delete_member(request: Request, igg_id: str):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    guild_id = require_guild(request)
    conn = get_conn()
    member = conn.execute("SELECT * FROM members WHERE igg_id = ? AND guild_id = ?", (igg_id, guild_id)).fetchone()
    conn.close()

    if not member:
        return HTMLResponse("<h2>Member not found</h2>", status_code=404)

    return templates.TemplateResponse(request, "confirm_delete_member.html", {
        "member": member,
        "is_admin": True
    })


@app.post("/member/{igg_id}/delete")
def archive_individual_member(
    request: Request,
    igg_id: str,
    removal_reason: str = Form(...),
    removal_notes: str = Form(""),
    confirm_text: str = Form(...)
):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM members WHERE igg_id = ?", (igg_id,))
    member = c.fetchone()

    if not member:
        conn.close()
        return HTMLResponse("<h2>Member not found</h2>", status_code=404)

    if confirm_text != member["name"]:
        conn.close()
        return HTMLResponse("<h2>Confirmation text did not match player name.</h2>", status_code=400)

    removed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    c.execute("""
        INSERT INTO former_members
        (igg_id, name, rank, might, kills, edm, mana, sigils, kingdom_limit, comments,
         alt_account, troop_comp, communication_method, whatsapp_number, discord_username,
         watchlist_flag, removal_reason, removal_notes, removed_at, original_created_at, original_updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        member["igg_id"], member["name"], member["rank"], member["might"], member["kills"], member["edm"],
        member["mana"], member["sigils"], member["kingdom_limit"], member["comments"],
        member["alt_account"], member["troop_comp"], member["communication_method"], member["whatsapp_number"],
        member["discord_username"], member["watchlist_flag"], removal_reason, removal_notes, removed_at,
        member["created_at"], member["updated_at"]
    ))

    c.execute("DELETE FROM members WHERE igg_id = ?", (igg_id,))
    conn.commit()
    conn.close()

    return RedirectResponse(url="/former-members", status_code=302)


@app.get("/former-members", response_class=HTMLResponse)
def former_members_page(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM former_members ORDER BY removed_at DESC, LOWER(name)")
    former_members = c.fetchall()
    conn.close()

    return templates.TemplateResponse(request, "former_members.html", {
        "former_members": former_members,
        "is_admin": True
    })


@app.post("/former-members/{former_id}/restore")
def restore_former_member(request: Request, former_id: int):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM former_members WHERE id = ?", (former_id,))
    former = c.fetchone()

    if not former:
        conn.close()
        return HTMLResponse("<h2>Former member not found</h2>", status_code=404)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    c.execute("""
        INSERT OR REPLACE INTO members
        (igg_id, name, rank, might, kills, edm, mana, sigils, kingdom_limit, comments,
         alt_account, troop_comp, communication_method, whatsapp_number, discord_username,
         watchlist_flag, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        former["igg_id"], former["name"], former["rank"], former["might"], former["kills"], former["edm"],
        former["mana"], former["sigils"], former["kingdom_limit"], former["comments"],
        former["alt_account"], former["troop_comp"], former["communication_method"], former["whatsapp_number"],
        former["discord_username"], former["watchlist_flag"], former["original_created_at"] or now, now
    ))

    c.execute("DELETE FROM former_members WHERE id = ?", (former_id,))
    conn.commit()
    conn.close()

    return RedirectResponse(url="/former-members", status_code=302)


@app.get("/former-members/{former_id}/delete", response_class=HTMLResponse)
def confirm_delete_former_member(request: Request, former_id: int):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM former_members WHERE id = ?", (former_id,))
    former = c.fetchone()
    conn.close()

    if not former:
        return HTMLResponse("<h2>Former member not found</h2>", status_code=404)

    return templates.TemplateResponse(request, "confirm_delete_former_member.html", {
        "former": former,
        "is_admin": True
    })


@app.post("/former-members/{former_id}/delete")
def permanently_delete_former_member(request: Request, former_id: int, confirm_text: str = Form(...)):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM former_members WHERE id = ?", (former_id,))
    former = c.fetchone()

    if not former:
        conn.close()
        return HTMLResponse("<h2>Former member not found</h2>", status_code=404)

    if confirm_text != former["name"]:
        conn.close()
        return HTMLResponse("<h2>Confirmation text did not match player name.</h2>", status_code=400)

    c.execute("DELETE FROM former_members WHERE id = ?", (former_id,))
    c.execute("DELETE FROM name_history WHERE igg_id = ?", (former["igg_id"],))
    conn.commit()
    conn.close()

    return RedirectResponse(url="/former-members", status_code=302)


@app.post("/member/{igg_id}/watchlist")
def toggle_watchlist(request: Request, igg_id: str, watchlist_flag: int = Form(...)):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    guild_id = require_guild(request)
    conn = get_conn()
    conn.execute(
        "UPDATE members SET watchlist_flag = ?, updated_at = ? WHERE igg_id = ? AND guild_id = ?",
        (1 if int(watchlist_flag) == 1 else 0, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), igg_id, guild_id)
    )
    conn.commit()
    conn.close()

    return RedirectResponse(url=f"/member/{igg_id}", status_code=302)


@app.get("/reports/archive", response_class=HTMLResponse)
def report_archive(request: Request):
    guild_id = require_guild(request)
    if not guild_id:
        return RedirectResponse(url="/", status_code=302)
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM kill_reports WHERE guild_id = ? ORDER BY generated_at DESC", (guild_id,))
    kill_reports = c.fetchall()
    c.execute("SELECT * FROM guild_fest_reports WHERE guild_id = ? ORDER BY generated_at DESC", (guild_id,))
    guild_fest_reports = c.fetchall()
    c.execute("SELECT * FROM guild_stat_snapshots WHERE guild_id = ? ORDER BY imported_at DESC", (guild_id,))
    snapshots = c.fetchall()
    conn.close()

    return templates.TemplateResponse(request, "report_archive.html", {
        "kill_reports": kill_reports,
        "guild_fest_reports": guild_fest_reports,
        "snapshots": snapshots,
        "is_admin": is_admin(request)
    })


@app.get("/reports/delete/{report_type}/{report_id}", response_class=HTMLResponse)
def confirm_delete_report_page(request: Request, report_type: str, report_id: int):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    if report_type not in ["kills", "guildfest"]:
        return HTMLResponse("<h2>Invalid report type</h2>", status_code=400)

    guild_id = require_guild(request)
    conn = get_conn()
    c = conn.cursor()
    if report_type == "kills":
        c.execute("SELECT * FROM kill_reports WHERE id = ? AND guild_id = ?", (report_id, guild_id))
    else:
        c.execute("SELECT * FROM guild_fest_reports WHERE id = ? AND guild_id = ?", (report_id, guild_id))
    report = c.fetchone()
    conn.close()

    if not report:
        return HTMLResponse("<h2>Report not found</h2>", status_code=404)

    return templates.TemplateResponse(request, "confirm_delete_report.html", {
        "report": report,
        "report_type": report_type,
        "is_admin": True
    })


@app.post("/reports/delete/{report_type}/{report_id}")
def delete_report(request: Request, report_type: str, report_id: int, confirm_text: str = Form(...)):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    guild_id = require_guild(request)
    conn = get_conn()
    c = conn.cursor()

    if report_type == "kills":
        c.execute("SELECT report_name FROM kill_reports WHERE id = ? AND guild_id = ?", (report_id, guild_id))
        report = c.fetchone()
        if not report or confirm_text != report["report_name"]:
            conn.close()
            return HTMLResponse("<h2>Confirmation text did not match report name.</h2>", status_code=400)
        c.execute("DELETE FROM kill_report_rows WHERE report_id = ? AND guild_id = ?", (report_id, guild_id))
        c.execute("DELETE FROM kill_reports WHERE id = ? AND guild_id = ?", (report_id, guild_id))
    elif report_type == "guildfest":
        c.execute("SELECT report_name FROM guild_fest_reports WHERE id = ? AND guild_id = ?", (report_id, guild_id))
        report = c.fetchone()
        if not report or confirm_text != report["report_name"]:
            conn.close()
            return HTMLResponse("<h2>Confirmation text did not match report name.</h2>", status_code=400)
        c.execute("DELETE FROM guild_fest_report_rows WHERE report_id = ? AND guild_id = ?", (report_id, guild_id))
        c.execute("DELETE FROM guild_fest_reports WHERE id = ? AND guild_id = ?", (report_id, guild_id))
    else:
        conn.close()
        return HTMLResponse("<h2>Invalid report type</h2>", status_code=400)

    conn.commit()
    conn.close()
    return RedirectResponse(url="/reports/archive", status_code=302)


@app.get("/snapshots/delete/{snapshot_id}", response_class=HTMLResponse)
def confirm_delete_snapshot_page(request: Request, snapshot_id: int):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    guild_id = require_guild(request)
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM guild_stat_snapshots WHERE id = ? AND guild_id = ?", (snapshot_id, guild_id))
    snapshot = c.fetchone()
    conn.close()

    if not snapshot:
        return HTMLResponse("<h2>Snapshot not found</h2>", status_code=404)

    return templates.TemplateResponse(request, "confirm_delete_snapshot.html", {
        "snapshot": snapshot,
        "is_admin": True
    })


@app.post("/snapshots/delete/{snapshot_id}")
def delete_snapshot(request: Request, snapshot_id: int, confirm_text: str = Form(...)):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    guild_id = require_guild(request)
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT snapshot_name FROM guild_stat_snapshots WHERE id = ? AND guild_id = ?", (snapshot_id, guild_id))
    snapshot = c.fetchone()

    if not snapshot or confirm_text != snapshot["snapshot_name"]:
        conn.close()
        return HTMLResponse("<h2>Confirmation text did not match snapshot name.</h2>", status_code=400)

    c.execute("DELETE FROM guild_stat_snapshot_rows WHERE snapshot_id = ? AND guild_id = ?", (snapshot_id, guild_id))
    c.execute("DELETE FROM guild_stat_snapshots WHERE id = ? AND guild_id = ?", (snapshot_id, guild_id))
    conn.commit()
    conn.close()

    return RedirectResponse(url="/reports/archive", status_code=302)


@app.get("/admin/delete-all", response_class=HTMLResponse)
def confirm_delete_all_page(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    return templates.TemplateResponse(request, "confirm_delete_all.html", {"is_admin": True})


@app.post("/admin/delete-all")
def delete_all_players(request: Request, confirm_text: str = Form(...)):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    if confirm_text != "DELETE ALL PLAYERS":
        return HTMLResponse("<h2>Confirmation text did not match. No players were deleted.</h2>", status_code=400)

    guild_id = require_guild(request)
    conn = get_conn()
    c = conn.cursor()
    for table in GUILD_DATA_TABLES:
        if table != "guild_settings":
            try:
                c.execute(f"DELETE FROM {table} WHERE guild_id = ?", (guild_id,))
            except Exception:
                pass
    c.execute("DELETE FROM guild_settings WHERE guild_id = ?", (guild_id,))
    conn.commit()
    conn.close()

    return RedirectResponse(url="/", status_code=302)


@app.get("/backup", response_class=HTMLResponse)
def backup_page(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    return templates.TemplateResponse(request, "backup_restore.html", {"is_admin": True})


@app.get("/backup/download")
def download_backup(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    return RedirectResponse(url="/data/export-excel", status_code=302)


@app.post("/backup/restore")
async def restore_backup(request: Request, file: UploadFile = File(...), confirm_text: str = Form(...)):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    return HTMLResponse("<h2>Database restore is disabled in multiguild mode. Use Excel import/export for guild-level data.</h2>", status_code=400)


@app.get("/data/export-excel")
def export_all_data_excel(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    guild_id = require_guild(request)
    conn = get_conn()
    output = BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for table in GUILD_DATA_TABLES:
            try:
                df = pd.read_sql_query(f"SELECT * FROM {table} WHERE guild_id = ?", conn, params=(guild_id,))
                df.to_excel(writer, sheet_name=table[:31], index=False)
            except Exception:
                pass

    conn.close()
    output.seek(0)

    filename = f"mj_guild_full_data_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


@app.post("/data/import-excel")
async def import_all_data_excel(request: Request, file: UploadFile = File(...), confirm_text: str = Form(...)):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    if confirm_text != "IMPORT EXCEL DATA":
        return HTMLResponse("<h2>Confirmation text did not match. Excel data was not imported.</h2>", status_code=400)

    filename = (file.filename or "").lower()
    if not filename.endswith(".xlsx"):
        return HTMLResponse("<h2>Please upload an .xlsx file exported from this app.</h2>", status_code=400)

    excel_data = pd.read_excel(file.file, sheet_name=None)
    guild_id = require_guild(request)
    conn = get_conn()
    c = conn.cursor()

    try:
        for table in reversed(GUILD_DATA_TABLES):
            c.execute(f"DELETE FROM {table} WHERE guild_id = ?", (guild_id,))

        for table in GUILD_DATA_TABLES:
            if table not in excel_data:
                continue

            df = excel_data[table]
            if df.empty:
                continue

            df = df.where(pd.notna(df), None)
            cols = [c for c in list(df.columns) if c != "guild_id"]
            if "guild_id" not in cols:
                pass
            cols = [col for col in cols if col != "guild_id"]
            placeholders = ",".join(["?"] * (len(cols) + 1))
            col_sql = "guild_id," + ",".join(cols)

            for _, row in df.iterrows():
                values = [guild_id] + [None if pd.isna(row[col]) else row[col] for col in cols]
                c.execute(f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})", values)

        conn.commit()
        conn.close()
        return RedirectResponse(url="/backup", status_code=302)

    except Exception as e:
        conn.rollback()
        conn.close()
        return HTMLResponse(f"<h2>Excel import failed: {e}</h2>", status_code=500)


@app.get("/snapshots/create")
def manual_snapshot(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    create_current_roster_snapshot(require_guild(request), source_filename="Manual snapshot from admin")
    return RedirectResponse(url="/reports/archive", status_code=302)


@app.get("/admin/login", response_class=HTMLResponse)
def admin_login_page(request: Request):
    if not current_guild_id(request):
        return RedirectResponse(url="/guild/login", status_code=302)
    return templates.TemplateResponse(request, "admin_login.html", {"error": "", "is_admin": is_admin(request)})


@app.post("/admin/login")
def admin_login(request: Request, password: str = Form(...)):
    guild_id = require_guild(request)
    if not guild_id:
        return RedirectResponse(url="/guild/login", status_code=302)
    conn = get_conn()
    guild = conn.execute("SELECT admin_password_hash FROM guilds WHERE id = ?", (guild_id,)).fetchone()
    conn.close()
    if guild and verify_password(password, guild["admin_password_hash"]):
        request.session["is_admin"] = True
        return RedirectResponse(url="/", status_code=302)

    return templates.TemplateResponse(request, "admin_login.html", {
        "error": "Incorrect admin password.",
        "is_admin": False
    }, status_code=401)


@app.get("/admin/logout")
def admin_logout(request: Request):
    gid = current_guild_id(request)
    tag = current_guild_tag(request)
    request.session.clear()
    if gid:
        request.session["guild_id"] = gid
        request.session["guild_tag"] = tag
    return RedirectResponse(url="/", status_code=302)


@app.get("/import", response_class=HTMLResponse)
def import_page(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    return templates.TemplateResponse(request, "import.html", {"is_admin": True})


@app.post("/import")
async def import_excel(request: Request, file: UploadFile = File(...)):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    filename = (file.filename or "").lower()
    if filename.endswith(".csv"):
        df = pd.read_csv(file.file)
    else:
        df = pd.read_excel(file.file)

    df.columns = [str(c).strip() for c in df.columns]
    required_columns = ["Name", "User ID", "Rank", "Might", "Kills", "Enemies Destroyed Might"]
    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        return HTMLResponse(f"""
        <html><body style="font-family: Arial; padding: 30px;">
        <h2>Import failed</h2>
        <p>Missing column(s): <strong>{", ".join(missing)}</strong></p>
        <p>Columns found:</p>
        <pre>{", ".join(df.columns)}</pre>
        <a href="/import">Back to import</a>
        </body></html>
        """, status_code=400)

    conn = get_conn()
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    snapshot_name = f"Guild Stats {now}"
    c.execute("""
        INSERT INTO guild_stat_snapshots (guild_id, snapshot_name, imported_at, source_filename)
        VALUES (?, ?, ?, ?)
    """, (guild_id, snapshot_name, now, file.filename))
    snapshot_id = c.lastrowid

    for _, row in df.iterrows():
        igg_id = str(row["User ID"]).strip()
        if not igg_id or igg_id.lower() == "nan":
            continue

        name = "" if pd.isna(row["Name"]) else str(row["Name"]).strip()
        rank = "" if pd.isna(row["Rank"]) else str(row["Rank"]).strip()
        might = 0 if pd.isna(row["Might"]) else int(float(row["Might"]))
        kills = 0 if pd.isna(row["Kills"]) else int(float(row["Kills"]))
        edm = 0 if pd.isna(row["Enemies Destroyed Might"]) else int(float(row["Enemies Destroyed Might"]))

        rank_map = {"R1": "RANK1", "R2": "RANK2", "R3": "RANK3", "R4": "RANK4", "R5": "RANK5"}
        rank = rank_map.get(rank, rank)

        c.execute("""
            INSERT INTO guild_stat_snapshot_rows
            (guild_id, snapshot_id, igg_id, player_name, rank, might, kills, edm)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (guild_id, snapshot_id, igg_id, name, rank, might, kills, edm))

        c.execute("SELECT * FROM members WHERE igg_id = ? AND guild_id = ?", (igg_id, guild_id))
        existing = c.fetchone()

        if existing:
            log_name_change(conn, guild_id, igg_id, existing["name"], name)
            c.execute("""
                UPDATE members
                SET name = ?, rank = ?, might = ?, kills = ?, edm = ?, updated_at = ?
                WHERE igg_id = ? AND guild_id = ?
            """, (name, rank, might, kills, edm, now, igg_id, guild_id))
        else:
            c.execute("""
                INSERT INTO pending_members
                (guild_id, igg_id, name, rank, might, kills, edm, source_filename, imported_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(igg_id) DO UPDATE SET
                    name = excluded.name,
                    rank = excluded.rank,
                    might = excluded.might,
                    kills = excluded.kills,
                    edm = excluded.edm,
                    source_filename = excluded.source_filename,
                    imported_at = excluded.imported_at
            """, (guild_id, igg_id, name, rank, might, kills, edm, file.filename, now))

    conn.commit()
    conn.close()
    return RedirectResponse(url="/", status_code=302)


@app.get("/reports/kills/create", response_class=HTMLResponse)
def create_kill_report_page(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    guild_id = require_guild(request)
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM guild_stat_snapshots WHERE guild_id = ? ORDER BY imported_at DESC", (guild_id,))
    snapshots = c.fetchall()
    conn.close()

    return templates.TemplateResponse(request, "create_kill_report.html", {
        "snapshots": snapshots,
        "is_admin": True
    })


@app.post("/reports/kills/create")
def create_kill_report(
    request: Request,
    report_name: str = Form(...),
    start_snapshot_id: int = Form(...),
    end_snapshot_id: int = Form(...),
    target_kill_increase: str = Form(""),
    target_edm_increase: str = Form(""),
    target_edm_per_kill: str = Form("")
):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    target_kill = int(target_kill_increase) if str(target_kill_increase).strip() else None
    target_edm = int(target_edm_increase) if str(target_edm_increase).strip() else None
    target_edm_pk = int(target_edm_per_kill) if str(target_edm_per_kill).strip() else None

    guild_id = require_guild(request)
    conn = get_conn()
    c = conn.cursor()

    c.execute("SELECT * FROM guild_stat_snapshot_rows WHERE snapshot_id = ? AND guild_id = ?", (start_snapshot_id, guild_id))
    start_rows = c.fetchall()

    c.execute("SELECT * FROM guild_stat_snapshot_rows WHERE snapshot_id = ? AND guild_id = ?", (end_snapshot_id, guild_id))
    end_rows = c.fetchall()

    start_map = {row["igg_id"]: row for row in start_rows}
    end_map = {row["igg_id"]: row for row in end_rows}
    c.execute("SELECT igg_id, name FROM members WHERE guild_id = ?", (guild_id,))
    active_members = {str(row["igg_id"]).strip(): row["name"] for row in c.fetchall() if row["igg_id"]}
    common_ids = sorted((set(start_map.keys()) & set(end_map.keys())) & set(active_members.keys()))

    report_rows = []

    for igg_id in common_ids:
        start_row = start_map[igg_id]
        end_row = end_map[igg_id]

        kill_increase = int(end_row["kills"] or 0) - int(start_row["kills"] or 0)
        edm_increase = int(end_row["edm"] or 0) - int(start_row["edm"] or 0)
        edm_per_kill = round(edm_increase / kill_increase) if kill_increase > 0 else 0

        pass_kills = None if target_kill is None else int(kill_increase >= target_kill)
        pass_edm = None if target_edm is None else int(edm_increase >= target_edm)
        pass_edm_pk = None if target_edm_pk is None else int(edm_per_kill >= target_edm_pk)

        checks = [x for x in [pass_kills, pass_edm, pass_edm_pk] if x is not None]
        overall_pass = None if not checks else int(all(x == 1 for x in checks))

        report_rows.append({
            "igg_id": igg_id,
            "player_name": active_members.get(igg_id) or end_row["player_name"],
            "kill_increase": kill_increase,
            "edm_increase": edm_increase,
            "edm_per_kill": edm_per_kill,
            "pass_kills": pass_kills,
            "pass_edm": pass_edm,
            "pass_edm_per_kill": pass_edm_pk,
            "overall_pass": overall_pass
        })

    avg_kills = round(sum(r["kill_increase"] for r in report_rows) / len(report_rows), 2) if report_rows else 0
    avg_edm = round(sum(r["edm_increase"] for r in report_rows) / len(report_rows), 2) if report_rows else 0
    avg_edm_pk = round(sum(r["edm_per_kill"] for r in report_rows) / len(report_rows), 2) if report_rows else 0
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    c.execute("""
        INSERT INTO kill_reports
        (guild_id, report_name, generated_at, start_snapshot_id, end_snapshot_id,
         target_kill_increase, target_edm_increase, target_edm_per_kill,
         avg_kill_increase, avg_edm_increase, avg_edm_per_kill)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        guild_id, report_name, generated_at, start_snapshot_id, end_snapshot_id,
        target_kill, target_edm, target_edm_pk, avg_kills, avg_edm, avg_edm_pk
    ))

    report_id = c.lastrowid

    for row in report_rows:
        c.execute("""
            INSERT INTO kill_report_rows
            (guild_id, report_id, igg_id, player_name, kill_increase, edm_increase, edm_per_kill,
             pass_kills, pass_edm, pass_edm_per_kill, overall_pass)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            guild_id, report_id, row["igg_id"], row["player_name"],
            row["kill_increase"], row["edm_increase"], row["edm_per_kill"],
            row["pass_kills"], row["pass_edm"], row["pass_edm_per_kill"], row["overall_pass"]
        ))

    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/reports/kills/{report_id}", status_code=302)


@app.get("/reports/kills/{report_id}", response_class=HTMLResponse)
def view_kill_report(request: Request, report_id: int):
    guild_id = require_guild(request)
    if not guild_id:
        return RedirectResponse(url="/", status_code=302)
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM kill_reports WHERE id = ? AND guild_id = ?", (report_id, guild_id))
    report = c.fetchone()
    c.execute("SELECT * FROM kill_report_rows WHERE report_id = ? AND guild_id = ? ORDER BY kill_increase DESC", (report_id, guild_id))
    rows = c.fetchall()
    conn.close()

    if not report:
        return HTMLResponse("<h2>Kill report not found</h2>", status_code=404)

    return templates.TemplateResponse(request, "kill_report.html", {
        "report": report,
        "rows": rows,
        "is_admin": is_admin(request)
    })


@app.get("/reports/guildfest/create", response_class=HTMLResponse)
def create_guild_fest_report_page(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    return templates.TemplateResponse(request, "create_guild_fest_report.html", {"is_admin": True})


@app.post("/reports/guildfest/create")
async def create_guild_fest_report(request: Request, report_name: str = Form(...), pass_score: int = Form(...), file: UploadFile = File(...)):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    filename = (file.filename or "").lower()
    df = pd.read_csv(file.file) if filename.endswith(".csv") else pd.read_excel(file.file)
    df.columns = [str(c).strip() for c in df.columns]

    required_columns = ["Name", "Completed", "Total", "Score", "Completed Bonus"]
    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        return HTMLResponse(f"<h2>Guild Fest import failed. Missing: {', '.join(missing)}</h2>", status_code=400)

    guild_id = require_guild(request)
    conn = get_conn()
    c = conn.cursor()
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    c.execute("SELECT name FROM members WHERE guild_id = ?", (guild_id,))
    active_names = {str(row["name"]).strip().lower(): row["name"] for row in c.fetchall() if row["name"]}
    guild_fest_rows = []

    for _, row in df.iterrows():
        player_name = "" if pd.isna(row["Name"]) else str(row["Name"]).strip()
        if not player_name:
            continue

        roster_name = active_names.get(player_name.lower())
        if not roster_name:
            continue

        completed = 0 if pd.isna(row["Completed"]) else int(float(row["Completed"]))
        total = 0 if pd.isna(row["Total"]) else int(float(row["Total"]))
        score = 0 if pd.isna(row["Score"]) else int(float(row["Score"]))
        completed_bonus = "" if pd.isna(row["Completed Bonus"]) else str(row["Completed Bonus"]).strip()
        passed = int(score >= pass_score)
        guild_fest_rows.append((roster_name, score, completed, total, completed_bonus, passed))

    avg_score = round(sum(row[1] for row in guild_fest_rows) / len(guild_fest_rows), 2) if guild_fest_rows else 0

    c.execute("""
        INSERT INTO guild_fest_reports
        (guild_id, report_name, generated_at, source_filename, pass_score, avg_score)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (guild_id, report_name, generated_at, file.filename, pass_score, avg_score))

    report_id = c.lastrowid

    for player_name, score, completed, total, completed_bonus, passed in guild_fest_rows:
        c.execute("""
            INSERT INTO guild_fest_report_rows
            (guild_id, report_id, player_name, guild_fest_score, completed, total, completed_bonus, passed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (guild_id, report_id, player_name, score, completed, total, completed_bonus, passed))

    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/reports/guildfest/{report_id}", status_code=302)


@app.get("/reports/guildfest/{report_id}", response_class=HTMLResponse)
def view_guild_fest_report(request: Request, report_id: int):
    guild_id = require_guild(request)
    if not guild_id:
        return RedirectResponse(url="/", status_code=302)
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM guild_fest_reports WHERE id = ? AND guild_id = ?", (report_id, guild_id))
    report = c.fetchone()
    c.execute("SELECT * FROM guild_fest_report_rows WHERE report_id = ? AND guild_id = ? ORDER BY guild_fest_score DESC", (report_id, guild_id))
    rows = c.fetchall()
    conn.close()

    if not report:
        return HTMLResponse("<h2>Guild Fest report not found</h2>", status_code=404)

    return templates.TemplateResponse(request, "guild_fest_report.html", {
        "report": report,
        "rows": rows,
        "is_admin": is_admin(request)
    })

