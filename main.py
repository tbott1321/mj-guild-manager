from fastapi import FastAPI, Request, Form, UploadFile, File, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from pathlib import Path
import sqlite3
import pandas as pd
from datetime import datetime
import os

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

    conn.commit()
    conn.close()


init_db()


def log_name_change(conn, igg_id, old, new):
    if old and new and old != new:
        conn.execute("""
            INSERT INTO name_history (igg_id, old_name, new_name, changed_at)
            VALUES (?, ?, ?, ?)
        """, (igg_id, old, new, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))


def is_admin(request: Request):
    return request.session.get("is_admin", False)


def get_sort_sql(sort_by: str, sort_dir: str):
    sort_map = {
        "name": "LOWER(COALESCE(name, ''))",
        "might": "COALESCE(might, 0)",
        "kills": "COALESCE(kills, 0)",
        "rank": "LOWER(COALESCE(rank, ''))"
    }

    sort_column = sort_map.get(sort_by, "COALESCE(might, 0)")
    direction = "ASC" if sort_dir == "asc" else "DESC"

    if sort_by == "name":
        return f"{sort_column} {direction}, COALESCE(might, 0) DESC"
    elif sort_by == "rank":
        return f"{sort_column} {direction}, LOWER(COALESCE(name, '')) ASC"
    else:
        return f"{sort_column} {direction}, LOWER(COALESCE(name, '')) ASC"


@app.get("/", response_class=HTMLResponse)
def dashboard(
    request: Request,
    search: str = Query(default=""),
    sort_by: str = Query(default="might"),
    sort_dir: str = Query(default="desc")
):
    conn = get_conn()
    c = conn.cursor()

    sql = """
        SELECT
            m.*,
            (
                SELECT MAX(changed_at)
                FROM name_history nh
                WHERE nh.igg_id = m.igg_id
            ) AS last_name_change
        FROM members m
    """

    params = []
    search = (search or "").strip()

    if search:
        sql += """
            WHERE
                LOWER(COALESCE(m.name, '')) LIKE ?
                OR LOWER(COALESCE(m.rank, '')) LIKE ?
                OR CAST(COALESCE(m.might, 0) AS TEXT) LIKE ?
                OR CAST(COALESCE(m.kills, 0) AS TEXT) LIKE ?
                OR CAST(COALESCE(m.edm, 0) AS TEXT) LIKE ?
                OR LOWER(COALESCE(m.comments, '')) LIKE ?
        """
        like_value = f"%{search.lower()}%"
        params.extend([like_value, like_value, like_value, like_value, like_value, like_value])

    sql += f" ORDER BY {get_sort_sql(sort_by, sort_dir)}"

    c.execute(sql, params)
    members = c.fetchall()
    conn.close()

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "members": members,
            "search": search,
            "sort_by": sort_by,
            "sort_dir": sort_dir,
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

    conn.close()

    return templates.TemplateResponse(
        request,
        "member.html",
        {
            "member": member,
            "history": history,
            "is_admin": is_admin(request)
        }
    )


@app.get("/member/{igg_id}/edit", response_class=HTMLResponse)
def edit_page(request: Request, igg_id: str):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM members WHERE igg_id = ?", (igg_id,))
    member = c.fetchone()
    conn.close()

    if not member:
        return HTMLResponse("<h2>Member not found</h2>", status_code=404)

    return templates.TemplateResponse(
        request,
        "edit_member.html",
        {
            "member": member,
            "is_admin": True
        }
    )


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
    comments: str = Form("")
):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    mana = max(0, min(6, int(mana)))
    sigils = max(0, int(sigils))

    conn = get_conn()
    c = conn.cursor()

    c.execute("SELECT name FROM members WHERE igg_id = ?", (igg_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        return HTMLResponse("<h2>Member not found</h2>", status_code=404)

    old = row["name"]
    log_name_change(conn, igg_id, old, name)

    c.execute("""
        UPDATE members
        SET name = ?, rank = ?, might = ?, kills = ?, edm = ?, mana = ?, sigils = ?, comments = ?, updated_at = ?
        WHERE igg_id = ?
    """, (
        name, rank, might, kills, edm, mana, sigils, comments,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"), igg_id
    ))

    conn.commit()
    conn.close()

    return RedirectResponse(url=f"/member/{igg_id}", status_code=302)


@app.post("/add")
def add_member(
    request: Request,
    name: str = Form(...),
    igg_id: str = Form(...),
    rank: str = Form(...),
    might: int = Form(...),
    kills: int = Form(...),
    edm: int = Form(...),
    mana: int = Form(0),
    sigils: int = Form(0),
    comments: str = Form("")
):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    mana = max(0, min(6, int(mana)))
    sigils = max(0, int(sigils))

    conn = get_conn()

    conn.execute("""
        INSERT OR REPLACE INTO members
        (igg_id, name, rank, might, kills, edm, mana, sigils, comments, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        igg_id, name, rank, might, kills, edm, mana, sigils, comments,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ))

    conn.commit()
    conn.close()

    return RedirectResponse(url="/", status_code=302)


@app.get("/delete/{igg_id}")
def delete_member(request: Request, igg_id: str):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    conn = get_conn()
    conn.execute("DELETE FROM members WHERE igg_id = ?", (igg_id,))
    conn.execute("DELETE FROM name_history WHERE igg_id = ?", (igg_id,))
    conn.commit()
    conn.close()

    return RedirectResponse(url="/", status_code=302)


@app.get("/import", response_class=HTMLResponse)
def import_page(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    return templates.TemplateResponse(
        request,
        "import.html",
        {
            "is_admin": True
        }
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

    required_columns = [
        "Name",
        "User ID",
        "Rank",
        "Might",
        "Kills",
        "Enemies Destroyed Might"
    ]

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        return HTMLResponse(f"""
        <html>
            <body style="font-family: Arial; padding: 30px;">
                <h2>Import failed</h2>
                <p>Missing column(s): <strong>{", ".join(missing)}</strong></p>
                <p>Columns found:</p>
                <pre>{", ".join(df.columns)}</pre>
                <a href="/import">Back to import</a>
            </body>
        </html>
        """, status_code=400)

    conn = get_conn()

    for _, row in df.iterrows():
        igg_id = str(row["User ID"]).strip()
        if not igg_id or igg_id.lower() == "nan":
            continue

        name = str(row["Name"]).strip() if not pd.isna(row["Name"]) else ""
        rank = str(row["Rank"]).strip() if not pd.isna(row["Rank"]) else ""
        might = 0 if pd.isna(row["Might"]) else int(float(row["Might"]))
        kills = 0 if pd.isna(row["Kills"]) else int(float(row["Kills"]))
        edm = 0 if pd.isna(row["Enemies Destroyed Might"]) else int(float(row["Enemies Destroyed Might"]))

        cur = conn.execute("SELECT * FROM members WHERE igg_id = ?", (igg_id,))
        existing = cur.fetchone()

        if existing:
            log_name_change(conn, igg_id, existing["name"], name)

            existing_comments = existing["comments"] or ""
            existing_mana = 0 if existing["mana"] is None else int(existing["mana"])
            existing_sigils = 0 if existing["sigils"] is None else int(existing["sigils"])

            conn.execute("""
                UPDATE members
                SET name = ?, rank = ?, might = ?, kills = ?, edm = ?, mana = ?, sigils = ?, comments = ?, updated_at = ?
                WHERE igg_id = ?
            """, (
                name, rank, might, kills, edm,
                existing_mana, existing_sigils, existing_comments,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                igg_id
            ))
        else:
            conn.execute("""
                INSERT INTO members
                (igg_id, name, rank, might, kills, edm, mana, sigils, comments, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                igg_id, name, rank, might, kills, edm,
                0, 0, "",
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ))

    conn.commit()
    conn.close()

    return RedirectResponse(url="/", status_code=302)