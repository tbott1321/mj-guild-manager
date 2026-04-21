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

# DATABASE PATH (LOCAL + RENDER)
RENDER_DISK_PATH = os.getenv("RENDER_DISK_PATH", "")
if RENDER_DISK_PATH:
    DB_PATH = os.path.join(RENDER_DISK_PATH, "database.db")
else:
    DB_PATH = str(BASE_DIR / "database.db")


# ---------------- DATABASE ----------------

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


# ---------------- AUTH ----------------

def is_admin(request: Request):
    return request.session.get("is_admin", False)


# ---------------- ROUTES ----------------

@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request, search: str = "", sort_by: str = "might", sort_dir: str = "desc"):
    conn = get_conn()
    c = conn.cursor()

    query = "SELECT * FROM members"
    params = []

    if search:
        query += " WHERE LOWER(name) LIKE ?"
        params.append(f"%{search.lower()}%")

    query += f" ORDER BY {sort_by} {'ASC' if sort_dir=='asc' else 'DESC'}"

    c.execute(query, params)
    members = c.fetchall()

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "members": members,
        "search": search,
        "sort_by": sort_by,
        "sort_dir": sort_dir,
        "is_admin": is_admin(request)
    })


# ---------------- ADMIN LOGIN ----------------

@app.get("/admin/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("admin_login.html", {
        "request": request,
        "error": ""
    })


@app.post("/admin/login")
def login(request: Request, password: str = Form(...)):
    ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")

    if password == ADMIN_PASSWORD:
        request.session["is_admin"] = True
        return RedirectResponse("/", status_code=302)

    return templates.TemplateResponse("admin_login.html", {
        "request": request,
        "error": "Incorrect password"
    })


@app.get("/admin/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/", status_code=302)


# ---------------- MEMBER PAGE ----------------

@app.get("/member/{igg_id}", response_class=HTMLResponse)
def member_page(request: Request, igg_id: str):
    conn = get_conn()
    c = conn.cursor()

    c.execute("SELECT * FROM members WHERE igg_id=?", (igg_id,))
    member = c.fetchone()

    c.execute("SELECT * FROM name_history WHERE igg_id=? ORDER BY changed_at DESC", (igg_id,))
    history = c.fetchall()

    return templates.TemplateResponse("member.html", {
        "request": request,
        "member": member,
        "history": history,
        "is_admin": is_admin(request)
    })


# ---------------- EDIT ----------------

@app.get("/member/{igg_id}/edit", response_class=HTMLResponse)
def edit_page(request: Request, igg_id: str):
    if not is_admin(request):
        return RedirectResponse("/admin/login")

    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM members WHERE igg_id=?", (igg_id,))
    member = c.fetchone()

    return templates.TemplateResponse("edit_member.html", {
        "request": request,
        "member": member
    })


@app.post("/member/{igg_id}/edit")
def edit_member(request: Request, igg_id: str,
                name: str = Form(...),
                rank: str = Form(...),
                might: int = Form(...),
                kills: int = Form(...),
                edm: int = Form(...),
                mana: int = Form(...),
                sigils: int = Form(...),
                comments: str = Form("")):

    if not is_admin(request):
        return RedirectResponse("/admin/login")

    conn = get_conn()
    c = conn.cursor()

    c.execute("SELECT name FROM members WHERE igg_id=?", (igg_id,))
    old = c.fetchone()["name"]

    log_name_change(conn, igg_id, old, name)

    c.execute("""
        UPDATE members
        SET name=?, rank=?, might=?, kills=?, edm=?, mana=?, sigils=?, comments=?, updated_at=?
        WHERE igg_id=?
    """, (name, rank, might, kills, edm, mana, sigils, comments,
          datetime.now().strftime("%Y-%m-%d %H:%M:%S"), igg_id))

    conn.commit()
    return RedirectResponse(f"/member/{igg_id}", status_code=302)


# ---------------- ADD ----------------

@app.post("/add")
def add_member(request: Request,
               name: str = Form(...),
               igg_id: str = Form(...),
               rank: str = Form(...),
               might: int = Form(...),
               kills: int = Form(...),
               edm: int = Form(...),
               mana: int = Form(0),
               sigils: int = Form(0),
               comments: str = Form("")):

    if not is_admin(request):
        return RedirectResponse("/admin/login")

    conn = get_conn()

    conn.execute("""
        INSERT OR REPLACE INTO members
        (igg_id, name, rank, might, kills, edm, mana, sigils, comments, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (igg_id, name, rank, might, kills, edm, mana, sigils, comments,
          datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
          datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    conn.commit()
    return RedirectResponse("/", status_code=302)


# ---------------- DELETE ----------------

@app.get("/delete/{igg_id}")
def delete_member(request: Request, igg_id: str):
    if not is_admin(request):
        return RedirectResponse("/admin/login")

    conn = get_conn()
    conn.execute("DELETE FROM members WHERE igg_id=?", (igg_id,))
    conn.execute("DELETE FROM name_history WHERE igg_id=?", (igg_id,))
    conn.commit()

    return RedirectResponse("/", status_code=302)


# ---------------- IMPORT ----------------

@app.get("/import", response_class=HTMLResponse)
def import_page(request: Request):
    if not is_admin(request):
        return RedirectResponse("/admin/login")

    return templates.TemplateResponse("import.html", {"request": request})


@app.post("/import")
async def import_excel(request: Request, file: UploadFile = File(...)):
    if not is_admin(request):
        return RedirectResponse("/admin/login")

    df = pd.read_excel(file.file)
    df.columns = [c.strip() for c in df.columns]

    conn = get_conn()

    for _, row in df.iterrows():
        igg_id = str(row["User ID"]).strip()
        name = str(row["Name"]).strip()
        rank = str(row["Rank"]).strip()
        might = int(row["Might"])
        kills = int(row["Kills"])
        edm = int(row["Enemies Destroyed Might"])

        cur = conn.execute("SELECT name FROM members WHERE igg_id=?", (igg_id,))
        existing = cur.fetchone()

        if existing:
            log_name_change(conn, igg_id, existing["name"], name)

            conn.execute("""
                UPDATE members
                SET name=?, rank=?, might=?, kills=?, edm=?, updated_at=?
                WHERE igg_id=?
            """, (name, rank, might, kills, edm,
                  datetime.now().strftime("%Y-%m-%d %H:%M:%S"), igg_id))
        else:
            conn.execute("""
                INSERT INTO members (igg_id, name, rank, might, kills, edm, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (igg_id, name, rank, might, kills, edm,
                  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                  datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    conn.commit()

    return RedirectResponse("/", status_code=302)