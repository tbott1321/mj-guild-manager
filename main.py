from fastapi import FastAPI, Request, Form, UploadFile, File, Query
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from pathlib import Path
import sqlite3
import pandas as pd
from datetime import datetime
import os
from io import BytesIO

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="super-secret-key-change-this")

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

RENDER_DISK_PATH = os.getenv("RENDER_DISK_PATH", "")
if RENDER_DISK_PATH:
    DB_PATH = os.path.join(RENDER_DISK_PATH, "database.db")
else:
    DB_PATH = str(BASE_DIR / "database.db")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def column_exists(conn, table_name, column_name):
    c = conn.cursor()
    c.execute(f"PRAGMA table_info({table_name})")
    cols = [row[1] for row in c.fetchall()]
    return column_name in cols


def init_db():
    conn = get_conn()
    c = conn.cursor()

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
            edm INTEGER,
            FOREIGN KEY(snapshot_id) REFERENCES guild_stat_snapshots(id)
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
            avg_edm_per_kill REAL,
            FOREIGN KEY(start_snapshot_id) REFERENCES guild_stat_snapshots(id),
            FOREIGN KEY(end_snapshot_id) REFERENCES guild_stat_snapshots(id)
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
            overall_pass INTEGER,
            FOREIGN KEY(report_id) REFERENCES kill_reports(id)
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
            passed INTEGER,
            FOREIGN KEY(report_id) REFERENCES guild_fest_reports(id)
        )
    """)

    if not column_exists(conn, "members", "mana"):
        c.execute("ALTER TABLE members ADD COLUMN mana INTEGER DEFAULT 0")
    if not column_exists(conn, "members", "sigils"):
        c.execute("ALTER TABLE members ADD COLUMN sigils INTEGER DEFAULT 0")
    if not column_exists(conn, "members", "comments"):
        c.execute("ALTER TABLE members ADD COLUMN comments TEXT")
    if not column_exists(conn, "members", "alt_account"):
        c.execute("ALTER TABLE members ADD COLUMN alt_account INTEGER DEFAULT 0")
    if not column_exists(conn, "members", "troop_comp"):
        c.execute("ALTER TABLE members ADD COLUMN troop_comp TEXT DEFAULT 'N/A'")
    if not column_exists(conn, "members", "communication_method"):
        c.execute("ALTER TABLE members ADD COLUMN communication_method TEXT DEFAULT 'N/A'")
    if not column_exists(conn, "members", "whatsapp_number"):
        c.execute("ALTER TABLE members ADD COLUMN whatsapp_number TEXT DEFAULT ''")
    if not column_exists(conn, "members", "discord_username"):
        c.execute("ALTER TABLE members ADD COLUMN discord_username TEXT DEFAULT ''")
    if not column_exists(conn, "members", "watchlist_flag"):
        c.execute("ALTER TABLE members ADD COLUMN watchlist_flag INTEGER DEFAULT 0")
    if not column_exists(conn, "members", "created_at"):
        c.execute("ALTER TABLE members ADD COLUMN created_at TEXT")
    if not column_exists(conn, "members", "updated_at"):
        c.execute("ALTER TABLE members ADD COLUMN updated_at TEXT")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    c.execute("UPDATE members SET rank = 'RANK1' WHERE rank IS NULL OR TRIM(rank) = ''")
    c.execute("UPDATE members SET rank = 'RANK1' WHERE rank = 'R1'")
    c.execute("UPDATE members SET rank = 'RANK2' WHERE rank = 'R2'")
    c.execute("UPDATE members SET rank = 'RANK3' WHERE rank = 'R3'")
    c.execute("UPDATE members SET rank = 'RANK4' WHERE rank = 'R4'")
    c.execute("UPDATE members SET rank = 'RANK5' WHERE rank = 'R5'")

    c.execute("UPDATE members SET mana = COALESCE(mana, 0) WHERE mana IS NULL")
    c.execute("UPDATE members SET sigils = COALESCE(sigils, 0) WHERE sigils IS NULL")
    c.execute("UPDATE members SET comments = COALESCE(comments, '') WHERE comments IS NULL")
    c.execute("UPDATE members SET alt_account = COALESCE(alt_account, 0) WHERE alt_account IS NULL")
    c.execute("UPDATE members SET troop_comp = COALESCE(troop_comp, 'N/A') WHERE troop_comp IS NULL OR TRIM(troop_comp) = ''")
    c.execute("UPDATE members SET communication_method = COALESCE(communication_method, 'N/A') WHERE communication_method IS NULL OR TRIM(communication_method) = ''")
    c.execute("UPDATE members SET whatsapp_number = COALESCE(whatsapp_number, '') WHERE whatsapp_number IS NULL")
    c.execute("UPDATE members SET discord_username = COALESCE(discord_username, '') WHERE discord_username IS NULL")
    c.execute("UPDATE members SET watchlist_flag = COALESCE(watchlist_flag, 0) WHERE watchlist_flag IS NULL")
    c.execute("UPDATE members SET created_at = COALESCE(created_at, ?) WHERE created_at IS NULL OR TRIM(created_at) = ''", (now,))
    c.execute("UPDATE members SET updated_at = COALESCE(updated_at, ?) WHERE updated_at IS NULL OR TRIM(updated_at) = ''", (now,))

    conn.commit()
    conn.close()


init_db()


def is_admin(request: Request):
    return request.session.get("is_admin", False)


def log_name_change(conn, igg_id, old, new):
    if old and new and old != new:
        conn.execute("""
            INSERT INTO name_history (igg_id, old_name, new_name, changed_at)
            VALUES (?, ?, ?, ?)
        """, (igg_id, old, new, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))


def normalise_comm_fields(method: str, whatsapp_number: str, discord_username: str):
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


def get_sort_sql(sort_by: str, sort_dir: str):
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
        "might": "COALESCE(might, 0)",
        "kills": "COALESCE(kills, 0)",
        "rank": rank_sort,
        "edm": "COALESCE(edm, 0)"
    }

    sort_column = sort_map.get(sort_by, "COALESCE(might, 0)")
    direction = "ASC" if sort_dir == "asc" else "DESC"

    if sort_by == "name":
        return f"{sort_column} {direction}, COALESCE(might, 0) DESC"
    elif sort_by == "rank":
        return f"{sort_column} {direction}, LOWER(COALESCE(name, '')) ASC"
    else:
        return f"{sort_column} {direction}, LOWER(COALESCE(name, '')) ASC"


def build_members_query(
    search: str = "",
    rank_filter: str = "",
    alt_filter: str = "",
    troop_comp_filter: str = "",
    min_mana: str = "",
    min_sigils: str = "",
    watchlist_only: str = "",
    sort_by: str = "might",
    sort_dir: str = "desc"
):
    sql = """
        SELECT
            m.*,
            (
                SELECT MAX(changed_at)
                FROM name_history nh
                WHERE nh.igg_id = m.igg_id
            ) AS last_name_change
        FROM members m
        WHERE 1=1
    """
    params = []

    search = (search or "").strip()
    if search:
        sql += """
            AND (
                LOWER(COALESCE(m.name, '')) LIKE ?
                OR LOWER(COALESCE(m.rank, '')) LIKE ?
                OR CAST(COALESCE(m.might, 0) AS TEXT) LIKE ?
                OR CAST(COALESCE(m.kills, 0) AS TEXT) LIKE ?
                OR CAST(COALESCE(m.edm, 0) AS TEXT) LIKE ?
            )
        """
        like_value = f"%{search.lower()}%"
        params.extend([like_value] * 5)

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


def get_member_fail_stats(conn, igg_id: str, member_name: str):
    c = conn.cursor()

    c.execute("""
        SELECT COUNT(*) AS total_count,
               SUM(CASE WHEN overall_pass = 0 THEN 1 ELSE 0 END) AS fail_count
        FROM kill_report_rows
        WHERE igg_id = ?
    """, (igg_id,))
    kill_stats = c.fetchone()

    c.execute("""
        SELECT COUNT(*) AS total_count,
               SUM(CASE WHEN passed = 0 THEN 1 ELSE 0 END) AS fail_count
        FROM guild_fest_report_rows
        WHERE LOWER(player_name) = LOWER(?)
    """, (member_name,))
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


@app.get("/", response_class=HTMLResponse)
def dashboard(
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
    conn = get_conn()
    c = conn.cursor()

    sql, params = build_members_query(
        search=search,
        rank_filter=rank_filter,
        alt_filter=alt_filter,
        troop_comp_filter=troop_comp_filter,
        min_mana=min_mana,
        min_sigils=min_sigils,
        watchlist_only=watchlist_only,
        sort_by=sort_by,
        sort_dir=sort_dir
    )
    c.execute(sql, params)
    members = c.fetchall()

    c.execute("SELECT * FROM kill_reports ORDER BY generated_at DESC LIMIT 1")
    latest_kill_report = c.fetchone()

    c.execute("SELECT * FROM guild_fest_reports ORDER BY generated_at DESC LIMIT 1")
    latest_guild_fest_report = c.fetchone()

    watchlist_summary = []
    if is_admin(request):
        c.execute("SELECT igg_id, name, watchlist_flag FROM members WHERE COALESCE(watchlist_flag, 0) = 1 ORDER BY LOWER(name)")
        watchlisted = c.fetchall()
        for member in watchlisted:
            stats = get_member_fail_stats(conn, member["igg_id"], member["name"])
            watchlist_summary.append({
                "igg_id": member["igg_id"],
                "name": member["name"],
                **stats
            })

    conn.close()

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "members": members,
            "search": search,
            "sort_by": sort_by,
            "sort_dir": sort_dir,
            "rank_filter": rank_filter,
            "alt_filter": alt_filter,
            "troop_comp_filter": troop_comp_filter,
            "min_mana": min_mana,
            "min_sigils": min_sigils,
            "watchlist_only": watchlist_only,
            "latest_kill_report": latest_kill_report,
            "latest_guild_fest_report": latest_guild_fest_report,
            "watchlist_summary": watchlist_summary,
            "is_admin": is_admin(request)
        }
    )


@app.post("/member/{igg_id}/watchlist")
def toggle_watchlist(
    request: Request,
    igg_id: str,
    watchlist_flag: int = Form(...)
):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    conn = get_conn()
    conn.execute(
        "UPDATE members SET watchlist_flag = ?, updated_at = ? WHERE igg_id = ?",
        (1 if int(watchlist_flag) == 1 else 0, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), igg_id)
    )
    conn.commit()
    conn.close()

    return RedirectResponse(url=f"/member/{igg_id}", status_code=302)


@app.get("/member/{igg_id}", response_class=HTMLResponse)
def member_page(request: Request, igg_id: str):
    conn = get_conn()
    c = conn.cursor()

    c.execute("SELECT * FROM members WHERE igg_id = ?", (igg_id,))
    member = c.fetchone()

    if not member:
        conn.close()
        return HTMLResponse("<h2>Member not found</h2>", status_code=404)

    c.execute("""
        SELECT * FROM name_history
        WHERE igg_id = ?
        ORDER BY changed_at DESC
    """, (igg_id,))
    history = c.fetchall()

    c.execute("""
        SELECT kr.generated_at, krr.player_name, krr.overall_pass
        FROM kill_report_rows krr
        JOIN kill_reports kr ON kr.id = krr.report_id
        WHERE krr.igg_id = ?
        ORDER BY kr.generated_at DESC
    """, (igg_id,))
    kill_history = c.fetchall()

    c.execute("""
        SELECT gfr.generated_at, gfr.report_name, gfrr.passed
        FROM guild_fest_report_rows gfrr
        JOIN guild_fest_reports gfr ON gfr.id = gfrr.report_id
        WHERE LOWER(gfrr.player_name) = LOWER(?)
        ORDER BY gfr.generated_at DESC
    """, (member["name"],))
    guild_fest_history = c.fetchall()

    fail_stats = get_member_fail_stats(conn, igg_id, member["name"])
    conn.close()

    return templates.TemplateResponse(
        request,
        "member.html",
        {
            "member": member,
            "history": history,
            "kill_history": kill_history,
            "guild_fest_history": guild_fest_history,
            "fail_stats": fail_stats,
            "is_admin": is_admin(request)
        }
    )


@app.get("/admin/login", response_class=HTMLResponse)
def admin_login_page(request: Request):
    return templates.TemplateResponse(
        request,
        "admin_login.html",
        {
            "error": "",
            "is_admin": is_admin(request)
        }
    )


@app.post("/admin/login")
def admin_login(request: Request, password: str = Form(...)):
    admin_password = os.getenv("ADMIN_PASSWORD", "admin123")

    if password == admin_password:
        request.session["is_admin"] = True
        return RedirectResponse(url="/", status_code=302)

    return templates.TemplateResponse(
        request,
        "admin_login.html",
        {
            "error": "Incorrect admin password.",
            "is_admin": False
        },
        status_code=401
    )


@app.get("/admin/logout")
def admin_logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/", status_code=302)


@app.get("/import", response_class=HTMLResponse)
def import_page(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    return templates.TemplateResponse(
        request,
        "import.html",
        {"is_admin": True}
    )


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
        INSERT INTO guild_stat_snapshots (snapshot_name, imported_at, source_filename)
        VALUES (?, ?, ?)
    """, (snapshot_name, now, file.filename))
    snapshot_id = c.lastrowid

    for _, row in df.iterrows():
        igg_id = str(row["User ID"]).strip()
        if not igg_id or igg_id.lower() == "nan":
            continue

        name = str(row["Name"]).strip() if not pd.isna(row["Name"]) else ""
        rank = str(row["Rank"]).strip() if not pd.isna(row["Rank"]) else ""
        might = 0 if pd.isna(row["Might"]) else int(float(row["Might"]))
        kills = 0 if pd.isna(row["Kills"]) else int(float(row["Kills"]))
        edm = 0 if pd.isna(row["Enemies Destroyed Might"]) else int(float(row["Enemies Destroyed Might"]))

        if rank == "R1":
            rank = "RANK1"
        elif rank == "R2":
            rank = "RANK2"
        elif rank == "R3":
            rank = "RANK3"
        elif rank == "R4":
            rank = "RANK4"
        elif rank == "R5":
            rank = "RANK5"

        c.execute("""
            INSERT INTO guild_stat_snapshot_rows
            (snapshot_id, igg_id, player_name, rank, might, kills, edm)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (snapshot_id, igg_id, name, rank, might, kills, edm))

        cur = conn.execute("SELECT * FROM members WHERE igg_id = ?", (igg_id,))
        existing = cur.fetchone()

        if existing:
            log_name_change(conn, igg_id, existing["name"], name)

            existing_comments = existing["comments"] or ""
            existing_mana = 0 if existing["mana"] is None else int(existing["mana"])
            existing_sigils = 0 if existing["sigils"] is None else int(existing["sigils"])
            existing_alt_account = 0 if existing["alt_account"] is None else int(existing["alt_account"])
            existing_troop_comp = existing["troop_comp"] or "N/A"
            existing_comm_method = existing["communication_method"] or "N/A"
            existing_whatsapp = existing["whatsapp_number"] or ""
            existing_discord = existing["discord_username"] or ""
            existing_watchlist = 0 if existing["watchlist_flag"] is None else int(existing["watchlist_flag"])

            conn.execute("""
                UPDATE members
                SET name = ?, rank = ?, might = ?, kills = ?, edm = ?, mana = ?, sigils = ?,
                    alt_account = ?, troop_comp = ?, communication_method = ?, whatsapp_number = ?, discord_username = ?,
                    watchlist_flag = ?, comments = ?, updated_at = ?
                WHERE igg_id = ?
            """, (
                name, rank, might, kills, edm, existing_mana, existing_sigils,
                existing_alt_account, existing_troop_comp, existing_comm_method, existing_whatsapp, existing_discord,
                existing_watchlist, existing_comments, now, igg_id
            ))
        else:
            conn.execute("""
                INSERT INTO members
                (igg_id, name, rank, might, kills, edm, mana, sigils, alt_account, troop_comp,
                 communication_method, whatsapp_number, discord_username, watchlist_flag, comments, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                igg_id, name, rank, might, kills, edm, 0, 0, 0, "N/A",
                "N/A", "", "", 0, "", now, now
            ))

    conn.commit()
    conn.close()

    return RedirectResponse(url="/", status_code=302)


@app.get("/reports/kills/create", response_class=HTMLResponse)
def create_kill_report_page(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM guild_stat_snapshots ORDER BY imported_at DESC")
    snapshots = c.fetchall()
    conn.close()

    return templates.TemplateResponse(
        request,
        "create_kill_report.html",
        {"snapshots": snapshots, "is_admin": True}
    )


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

    conn = get_conn()
    c = conn.cursor()

    c.execute("SELECT * FROM guild_stat_snapshot_rows WHERE snapshot_id = ?", (start_snapshot_id,))
    start_rows = c.fetchall()
    c.execute("SELECT * FROM guild_stat_snapshot_rows WHERE snapshot_id = ?", (end_snapshot_id,))
    end_rows = c.fetchall()

    start_map = {row["igg_id"]: row for row in start_rows}
    end_map = {row["igg_id"]: row for row in end_rows}
    common_ids = sorted(set(start_map.keys()) & set(end_map.keys()))

    report_rows = []
    for igg_id in common_ids:
        start_row = start_map[igg_id]
        end_row = end_map[igg_id]

        kill_increase = int(end_row["kills"] or 0) - int(start_row["kills"] or 0)
        edm_increase = int(end_row["edm"] or 0) - int(start_row["edm"] or 0)

        edm_per_kill = 0
        if kill_increase > 0:
            edm_per_kill = round(edm_increase / kill_increase)

        pass_kills = None if target_kill is None else int(kill_increase >= target_kill)
        pass_edm = None if target_edm is None else int(edm_increase >= target_edm)
        pass_edm_pk = None if target_edm_pk is None else int(edm_per_kill >= target_edm_pk)

        checks = [x for x in [pass_kills, pass_edm, pass_edm_pk] if x is not None]
        overall_pass = None if not checks else int(all(x == 1 for x in checks))

        report_rows.append({
            "igg_id": igg_id,
            "player_name": end_row["player_name"],
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
        (report_name, generated_at, start_snapshot_id, end_snapshot_id,
         target_kill_increase, target_edm_increase, target_edm_per_kill,
         avg_kill_increase, avg_edm_increase, avg_edm_per_kill)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        report_name, generated_at, start_snapshot_id, end_snapshot_id,
        target_kill, target_edm, target_edm_pk,
        avg_kills, avg_edm, avg_edm_pk
    ))
    report_id = c.lastrowid

    for row in report_rows:
        c.execute("""
            INSERT INTO kill_report_rows
            (report_id, igg_id, player_name, kill_increase, edm_increase, edm_per_kill,
             pass_kills, pass_edm, pass_edm_per_kill, overall_pass)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            report_id, row["igg_id"], row["player_name"],
            row["kill_increase"], row["edm_increase"], row["edm_per_kill"],
            row["pass_kills"], row["pass_edm"], row["pass_edm_per_kill"], row["overall_pass"]
        ))

    conn.commit()
    conn.close()

    return RedirectResponse(url=f"/reports/kills/{report_id}", status_code=302)


@app.get("/reports/kills/{report_id}", response_class=HTMLResponse)
def view_kill_report(request: Request, report_id: int):
    conn = get_conn()
    c = conn.cursor()

    c.execute("SELECT * FROM kill_reports WHERE id = ?", (report_id,))
    report = c.fetchone()
    c.execute("SELECT * FROM kill_report_rows WHERE report_id = ? ORDER BY kill_increase DESC", (report_id,))
    rows = c.fetchall()

    conn.close()

    if not report:
        return HTMLResponse("<h2>Kill report not found</h2>", status_code=404)

    return templates.TemplateResponse(
        request,
        "kill_report.html",
        {"report": report, "rows": rows, "is_admin": is_admin(request)}
    )


@app.get("/reports/guildfest/create", response_class=HTMLResponse)
def create_guild_fest_report_page(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    return templates.TemplateResponse(
        request,
        "create_guild_fest_report.html",
        {"is_admin": True}
    )


@app.post("/reports/guildfest/create")
async def create_guild_fest_report(
    request: Request,
    report_name: str = Form(...),
    pass_score: int = Form(...),
    file: UploadFile = File(...)
):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    filename = (file.filename or "").lower()
    if filename.endswith(".csv"):
        df = pd.read_csv(file.file)
    else:
        df = pd.read_excel(file.file)

    df.columns = [str(c).strip() for c in df.columns]

    required_columns = ["Name", "Completed", "Total", "Score", "Completed Bonus"]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        return HTMLResponse(f"""
        <html><body style="font-family: Arial; padding: 30px;">
        <h2>Guild Fest import failed</h2>
        <p>Missing column(s): <strong>{", ".join(missing)}</strong></p>
        <p>Columns found:</p>
        <pre>{", ".join(df.columns)}</pre>
        <a href="/reports/guildfest/create">Back</a>
        </body></html>
        """, status_code=400)

    conn = get_conn()
    c = conn.cursor()

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    avg_score = round(float(df["Score"].fillna(0).mean()), 2) if len(df.index) > 0 else 0

    c.execute("""
        INSERT INTO guild_fest_reports
        (report_name, generated_at, source_filename, pass_score, avg_score)
        VALUES (?, ?, ?, ?, ?)
    """, (report_name, generated_at, file.filename, pass_score, avg_score))
    report_id = c.lastrowid

    for _, row in df.iterrows():
        player_name = "" if pd.isna(row["Name"]) else str(row["Name"]).strip()
        if not player_name:
            continue

        completed = 0 if pd.isna(row["Completed"]) else int(float(row["Completed"]))
        total = 0 if pd.isna(row["Total"]) else int(float(row["Total"]))
        score = 0 if pd.isna(row["Score"]) else int(float(row["Score"]))
        completed_bonus = "" if pd.isna(row["Completed Bonus"]) else str(row["Completed Bonus"]).strip()
        passed = int(score >= pass_score)

        c.execute("""
            INSERT INTO guild_fest_report_rows
            (report_id, player_name, guild_fest_score, completed, total, completed_bonus, passed)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (report_id, player_name, score, completed, total, completed_bonus, passed))

    conn.commit()
    conn.close()

    return RedirectResponse(url=f"/reports/guildfest/{report_id}", status_code=302)


@app.get("/reports/guildfest/{report_id}", response_class=HTMLResponse)
def view_guild_fest_report(request: Request, report_id: int):
    conn = get_conn()
    c = conn.cursor()

    c.execute("SELECT * FROM guild_fest_reports WHERE id = ?", (report_id,))
    report = c.fetchone()
    c.execute("""
        SELECT * FROM guild_fest_report_rows
        WHERE report_id = ?
        ORDER BY guild_fest_score DESC
    """, (report_id,))
    rows = c.fetchall()

    conn.close()

    if not report:
        return HTMLResponse("<h2>Guild Fest report not found</h2>", status_code=404)

    return templates.TemplateResponse(
        request,
        "guild_fest_report.html",
        {"report": report, "rows": rows, "is_admin": is_admin(request)}
    )


@app.get("/export")
def export_members(
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
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    conn = get_conn()
    sql, params = build_members_query(
        search=search,
        rank_filter=rank_filter,
        alt_filter=alt_filter,
        troop_comp_filter=troop_comp_filter,
        min_mana=min_mana,
        min_sigils=min_sigils,
        watchlist_only=watchlist_only,
        sort_by=sort_by,
        sort_dir=sort_dir
    )
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()

    export_cols = [
        "name", "igg_id", "rank", "might", "kills", "edm",
        "mana", "sigils", "alt_account", "troop_comp",
        "communication_method", "whatsapp_number", "discord_username",
        "watchlist_flag", "comments", "created_at", "updated_at", "last_name_change"
    ]
    df = df[[col for col in export_cols if col in df.columns]]

    if "alt_account" in df.columns:
        df["alt_account"] = df["alt_account"].apply(lambda x: "Yes" if int(x or 0) == 1 else "No")
    if "watchlist_flag" in df.columns:
        df["watchlist_flag"] = df["watchlist_flag"].apply(lambda x: "Yes" if int(x or 0) == 1 else "No")

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Members")

    output.seek(0)
    filename = f"mj_guild_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )