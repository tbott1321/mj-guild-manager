from pathlib import Path
import re
import shutil
import py_compile

PROJECT = Path.cwd()
MAIN = PROJECT / "main.py"
REQ = PROJECT / "requirements.txt"
TEMPLATES = PROJECT / "templates"

if not MAIN.exists():
    raise SystemExit("main.py not found. Run this script from your project folder.")

backup = PROJECT / "main.py.backup_before_stripe_billing"
if not backup.exists():
    shutil.copyfile(MAIN, backup)

text = MAIN.read_text(encoding="utf-8-sig")

# -----------------------------------------------------------------------------
# 1) Imports + Stripe constants
# -----------------------------------------------------------------------------
if "import stripe" not in text:
    text = text.replace("import re\n", "import re\nimport stripe\n", 1) if "import re\n" in text else text.replace("from io import BytesIO\n", "from io import BytesIO\nimport stripe\n", 1)

const_marker = 'DEFAULT_MJ_ADMIN_PASSWORD = os.getenv("DEFAULT_MJ_ADMIN_PASSWORD", "admin123")\n'
stripe_consts = r'''

# Stripe billing configuration
# Create these as recurring Stripe Prices:
# - monthly: £9.99 / month
# - six_month: £49.99 / 6 months
# - twelve_month: £84.99 / 12 months
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
STRIPE_PRICE_MONTHLY_ID = os.getenv("STRIPE_PRICE_MONTHLY_ID", "")
STRIPE_PRICE_6_MONTH_ID = os.getenv("STRIPE_PRICE_6_MONTH_ID", "")
STRIPE_PRICE_12_MONTH_ID = os.getenv("STRIPE_PRICE_12_MONTH_ID", "")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "http://127.0.0.1:8000").rstrip("/")

if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

BILLING_PLANS = {
    "monthly": {
        "label": "Monthly",
        "display_price": "£9.99 / month",
        "price_id_env": "STRIPE_PRICE_MONTHLY_ID",
        "price_id": STRIPE_PRICE_MONTHLY_ID,
    },
    "six_month": {
        "label": "6 Months",
        "display_price": "£49.99 / 6 months",
        "price_id_env": "STRIPE_PRICE_6_MONTH_ID",
        "price_id": STRIPE_PRICE_6_MONTH_ID,
    },
    "twelve_month": {
        "label": "12 Months",
        "display_price": "£84.99 / 12 months",
        "price_id_env": "STRIPE_PRICE_12_MONTH_ID",
        "price_id": STRIPE_PRICE_12_MONTH_ID,
    },
}

BILLING_ALLOWED_STATUSES = {"active", "trialing", "manual_active"}
'''
if "STRIPE_PRICE_MONTHLY_ID" not in text:
    if const_marker not in text:
        raise SystemExit("Could not find DEFAULT_MJ_ADMIN_PASSWORD marker in main.py")
    text = text.replace(const_marker, const_marker + stripe_consts, 1)

# -----------------------------------------------------------------------------
# 2) Billing helpers + middleware
# -----------------------------------------------------------------------------
helper_marker = 'def is_site_admin(request: Request):\n    return request.session.get("site_admin", False)\n'
helpers = r'''


def dt_from_unix(value):
    if not value:
        return None
    try:
        return datetime.fromtimestamp(int(value)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def get_billing_plan(plan_key: str):
    return BILLING_PLANS.get(plan_key or "monthly", BILLING_PLANS["monthly"])


def billing_status_label(status: str, manual_access=0):
    if int(manual_access or 0) == 1:
        return "Manual Active"
    labels = {
        "not_started": "Not Started",
        "pending_billing": "Pending Billing",
        "trialing": "Trialing",
        "active": "Active",
        "past_due": "Past Due",
        "unpaid": "Unpaid",
        "canceled": "Canceled",
        "incomplete": "Incomplete",
        "incomplete_expired": "Incomplete Expired",
        "manual_active": "Manual Active",
    }
    return labels.get(status or "not_started", status or "Not Started")


def guild_billing_allowed(guild):
    if not guild:
        return False, "Guild not found."
    if int(guild["is_disabled"] or 0) == 1:
        reason = (guild["disabled_reason"] or "This guild has been disabled by site admin.").strip()
        return False, f"Guild is disabled. {reason}"
    if int(guild["manual_access"] or 0) == 1:
        return True, ""
    status = guild["subscription_status"] or "not_started"
    if status in BILLING_ALLOWED_STATUSES:
        return True, ""
    if status == "pending_billing":
        return False, "Billing is not complete for this guild. Please complete checkout or contact support."
    if status in {"canceled", "unpaid", "past_due", "incomplete", "incomplete_expired"}:
        return False, f"Guild subscription is {billing_status_label(status)}. Please update billing or contact support."
    return False, "Guild billing is not active. Please complete checkout or contact support."


def record_payment_event(conn, guild_id, event_type, status="", amount=None, currency="", description="", stripe_event_id="", stripe_invoice_id="", stripe_subscription_id=""):
    conn.execute("""
        INSERT INTO guild_payment_events
        (guild_id, event_type, status, amount, currency, description, stripe_event_id, stripe_invoice_id, stripe_subscription_id, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        guild_id,
        event_type,
        status or "",
        amount,
        (currency or "").upper(),
        description or "",
        stripe_event_id or "",
        stripe_invoice_id or "",
        stripe_subscription_id or "",
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    ))


async def guild_billing_guard(request: Request, call_next):
    path = request.url.path
    exempt_prefixes = (
        "/guild/login", "/guild/create", "/guild/logout",
        "/billing/checkout", "/billing/success", "/billing/cancel",
        "/stripe/webhook", "/site-admin", "/favicon.ico"
    )
    if path == "/" or path.startswith(exempt_prefixes):
        return await call_next(request)
    guild_id = request.session.get("guild_id")
    if guild_id:
        conn = get_conn()
        guild = conn.execute("SELECT * FROM guilds WHERE id = ?", (guild_id,)).fetchone()
        conn.close()
        allowed, message = guild_billing_allowed(guild)
        if not allowed:
            request.session.clear()
            return templates.TemplateResponse(request, "guild_login.html", {"error": message}, status_code=403)
    return await call_next(request)


if not any(getattr(mw, "cls", None).__name__ == "BaseHTTPMiddleware" for mw in app.user_middleware):
    app.middleware("http")(guild_billing_guard)
'''
if "def guild_billing_allowed" not in text:
    if helper_marker not in text:
        raise SystemExit("Could not find is_site_admin helper marker in main.py")
    text = text.replace(helper_marker, helper_marker + helpers, 1)

# -----------------------------------------------------------------------------
# 3) Database migration columns + payment events table
# -----------------------------------------------------------------------------
old_cols = '''    for col, definition in {
        "guild_password_plain": "TEXT DEFAULT ''",
        "admin_password_plain": "TEXT DEFAULT ''",
        "is_disabled": "INTEGER DEFAULT 0",
        "disabled_reason": "TEXT DEFAULT ''",
    }.items():
        if not column_exists(conn, "guilds", col):
            c.execute(f"ALTER TABLE guilds ADD COLUMN {col} {definition}")
'''
new_cols = '''    for col, definition in {
        "guild_password_plain": "TEXT DEFAULT ''",
        "admin_password_plain": "TEXT DEFAULT ''",
        "is_disabled": "INTEGER DEFAULT 0",
        "disabled_reason": "TEXT DEFAULT ''",
        "billing_email": "TEXT DEFAULT ''",
        "stripe_customer_id": "TEXT DEFAULT ''",
        "stripe_subscription_id": "TEXT DEFAULT ''",
        "stripe_price_id": "TEXT DEFAULT ''",
        "stripe_plan": "TEXT DEFAULT 'monthly'",
        "subscription_status": "TEXT DEFAULT 'not_started'",
        "trial_ends_at": "TEXT DEFAULT ''",
        "current_period_end": "TEXT DEFAULT ''",
        "manual_access": "INTEGER DEFAULT 0",
        "manual_access_reason": "TEXT DEFAULT ''",
        "last_payment_at": "TEXT DEFAULT ''",
        "last_payment_amount": "INTEGER DEFAULT 0",
        "last_payment_currency": "TEXT DEFAULT ''",
    }.items():
        if not column_exists(conn, "guilds", col):
            c.execute(f"ALTER TABLE guilds ADD COLUMN {col} {definition}")

    c.execute("""
        CREATE TABLE IF NOT EXISTS guild_payment_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER,
            event_type TEXT,
            status TEXT,
            amount INTEGER,
            currency TEXT,
            description TEXT,
            stripe_event_id TEXT,
            stripe_invoice_id TEXT,
            stripe_subscription_id TEXT,
            created_at TEXT
        )
    """)
'''
if "guild_payment_events" not in text:
    if old_cols not in text:
        raise SystemExit("Could not find guilds column migration block in main.py")
    text = text.replace(old_cols, new_cols, 1)

# Set existing M/J to manual active in the default update block
old_update = '''            admin_password_plain = CASE WHEN admin_password_plain IS NULL OR TRIM(admin_password_plain) = '' THEN ? ELSE admin_password_plain END,
            is_disabled = COALESCE(is_disabled, 0),
            disabled_reason = COALESCE(disabled_reason, '')
        WHERE guild_tag = ? COLLATE BINARY
    """, (DEFAULT_MJ_GUILD_PASSWORD, DEFAULT_MJ_ADMIN_PASSWORD, "M/J"))
'''
new_update = '''            admin_password_plain = CASE WHEN admin_password_plain IS NULL OR TRIM(admin_password_plain) = '' THEN ? ELSE admin_password_plain END,
            is_disabled = COALESCE(is_disabled, 0),
            disabled_reason = COALESCE(disabled_reason, ''),
            billing_email = CASE WHEN billing_email IS NULL OR TRIM(billing_email) = '' THEN email ELSE billing_email END,
            stripe_plan = COALESCE(stripe_plan, 'monthly'),
            subscription_status = CASE WHEN subscription_status IS NULL OR TRIM(subscription_status) = '' OR subscription_status = 'not_started' THEN 'manual_active' ELSE subscription_status END,
            manual_access = CASE WHEN manual_access IS NULL THEN 1 ELSE manual_access END,
            manual_access_reason = CASE WHEN manual_access_reason IS NULL OR TRIM(manual_access_reason) = '' THEN 'Existing M/J guild bypass' ELSE manual_access_reason END
        WHERE guild_tag = ? COLLATE BINARY
    """, (DEFAULT_MJ_GUILD_PASSWORD, DEFAULT_MJ_ADMIN_PASSWORD, "M/J"))
'''
if "Existing M/J guild bypass" not in text and old_update in text:
    text = text.replace(old_update, new_update, 1)

# -----------------------------------------------------------------------------
# 4) Login guard and create guild billing flow
# -----------------------------------------------------------------------------
old_login_check = '''    if int(guild["is_disabled"] or 0) == 1:
        reason = (guild["disabled_reason"] or "This guild has been disabled by site admin.").strip()
        return templates.TemplateResponse(request, "guild_login.html", {"error": f"Guild is disabled. {reason}"}, status_code=403)
    request.session.clear()
'''
new_login_check = '''    allowed, billing_message = guild_billing_allowed(guild)
    if not allowed:
        return templates.TemplateResponse(request, "guild_login.html", {"error": billing_message}, status_code=403)
    request.session.clear()
'''
if old_login_check in text:
    text = text.replace(old_login_check, new_login_check, 1)

text = text.replace('return templates.TemplateResponse(request, "create_guild.html", {"error": ""})', 'return templates.TemplateResponse(request, "create_guild.html", {"error": "", "plans": BILLING_PLANS})')
text = text.replace('{"error": "Guild tag must be exactly 3 printable, non-space characters."}', '{"error": "Guild tag must be exactly 3 printable, non-space characters.", "plans": BILLING_PLANS}')
text = text.replace('{"error": "Email addresses do not match."}', '{"error": "Email addresses do not match.", "plans": BILLING_PLANS}')
text = text.replace('{"error": "Guild and admin passwords are required."}', '{"error": "Guild and admin passwords are required.", "plans": BILLING_PLANS}')
text = text.replace('{"error": "That guild tag already exists."}', '{"error": "That guild tag already exists.", "plans": BILLING_PLANS}')

old_sig = '''def create_guild(
    request: Request,
    guild_tag: str = Form(...),
    email: str = Form(...),
    confirm_email: str = Form(...),
    guild_password: str = Form(...),
    admin_password: str = Form(...)
):'''
new_sig = '''def create_guild(
    request: Request,
    guild_tag: str = Form(...),
    email: str = Form(...),
    confirm_email: str = Form(...),
    guild_password: str = Form(...),
    admin_password: str = Form(...),
    billing_plan: str = Form("monthly")
):'''
if old_sig in text:
    text = text.replace(old_sig, new_sig, 1)

old_insert = '''        c.execute("""
            INSERT INTO guilds (guild_tag, email, guild_password_hash, admin_password_hash, guild_password_plain, admin_password_plain, is_disabled, disabled_reason, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (guild_tag, email, hash_password(guild_password), hash_password(admin_password), guild_password, admin_password, 0, "", now, now))
        guild_id = c.lastrowid
        conn.commit()
'''
new_insert = '''        plan = get_billing_plan(billing_plan)
        c.execute("""
            INSERT INTO guilds (
                guild_tag, email, billing_email, guild_password_hash, admin_password_hash,
                guild_password_plain, admin_password_plain, is_disabled, disabled_reason,
                stripe_plan, stripe_price_id, subscription_status, manual_access,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            guild_tag, email, email, hash_password(guild_password), hash_password(admin_password),
            guild_password, admin_password, 0, "",
            billing_plan, plan["price_id"], "pending_billing", 0,
            now, now
        ))
        guild_id = c.lastrowid
        conn.commit()
'''
if old_insert in text:
    text = text.replace(old_insert, new_insert, 1)

old_post_create = '''    conn.close()
    request.session.clear()
    request.session["guild_id"] = guild_id
    request.session["guild_tag"] = guild_tag
    request.session["is_admin"] = True
    return RedirectResponse(url="/", status_code=302)
'''
new_post_create = '''    conn.close()
    request.session.clear()
    request.session["pending_guild_id"] = guild_id
    return RedirectResponse(url=f"/billing/checkout/{guild_id}", status_code=302)
'''
if old_post_create in text:
    text = text.replace(old_post_create, new_post_create, 1)

# -----------------------------------------------------------------------------
# 5) Billing routes and webhooks
# -----------------------------------------------------------------------------
routes_marker = '@app.get("/guild/logout")\ndef guild_logout(request: Request):\n'
billing_routes = r'''

@app.get("/billing/checkout/{guild_id}")
def billing_checkout(request: Request, guild_id: int):
    pending_id = request.session.get("pending_guild_id")
    if not is_site_admin(request) and int(pending_id or 0) != int(guild_id):
        return HTMLResponse("<h2>Unauthorised billing session.</h2>", status_code=403)

    if not STRIPE_SECRET_KEY:
        return HTMLResponse("<h2>Stripe is not configured. Add STRIPE_SECRET_KEY and price IDs in Render, or manually activate this guild from Site Admin.</h2>", status_code=500)

    conn = get_conn()
    guild = conn.execute("SELECT * FROM guilds WHERE id = ?", (guild_id,)).fetchone()
    if not guild:
        conn.close()
        return HTMLResponse("<h2>Guild not found.</h2>", status_code=404)

    plan_key = guild["stripe_plan"] or "monthly"
    plan = get_billing_plan(plan_key)
    price_id = guild["stripe_price_id"] or plan["price_id"]
    if not price_id:
        conn.close()
        return HTMLResponse(f"<h2>Stripe price ID missing for {plan['label']}.</h2><p>Set {plan['price_id_env']} in Render.</p>", status_code=500)

    customer_id = guild["stripe_customer_id"] or ""
    if not customer_id:
        customer = stripe.Customer.create(
            email=guild["billing_email"] or guild["email"],
            metadata={"guild_id": str(guild_id), "guild_tag": guild["guild_tag"]}
        )
        customer_id = customer.id
        conn.execute("UPDATE guilds SET stripe_customer_id = ?, updated_at = ? WHERE id = ?", (customer_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), guild_id))
        conn.commit()

    session = stripe.checkout.Session.create(
        mode="subscription",
        customer=customer_id,
        line_items=[{"price": price_id, "quantity": 1}],
        subscription_data={
            "trial_period_days": 14,
            "metadata": {"guild_id": str(guild_id), "guild_tag": guild["guild_tag"], "plan": plan_key},
        },
        metadata={"guild_id": str(guild_id), "guild_tag": guild["guild_tag"], "plan": plan_key},
        payment_method_collection="always",
        success_url=f"{PUBLIC_BASE_URL}/billing/success?session_id={{CHECKOUT_SESSION_ID}}",
        cancel_url=f"{PUBLIC_BASE_URL}/billing/cancel",
    )

    conn.execute("""
        UPDATE guilds
        SET subscription_status = 'pending_billing', stripe_price_id = ?, stripe_plan = ?, updated_at = ?
        WHERE id = ?
    """, (price_id, plan_key, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), guild_id))
    record_payment_event(conn, guild_id, "checkout.created", "pending_billing", None, "GBP", f"Checkout started for {plan['display_price']}", "", "", "")
    conn.commit()
    conn.close()
    return RedirectResponse(session.url, status_code=303)


@app.get("/billing/success", response_class=HTMLResponse)
def billing_success(request: Request, session_id: str = Query(default="")):
    return HTMLResponse("""
    <html><body style='font-family:Segoe UI,Arial;padding:30px;'>
    <h2>Billing setup started</h2>
    <p>Your guild trial will activate once Stripe confirms the checkout. This usually happens immediately.</p>
    <p><a href='/guild/login'>Log in to Guild</a></p>
    </body></html>
    """)


@app.get("/billing/cancel", response_class=HTMLResponse)
def billing_cancel(request: Request):
    return HTMLResponse("""
    <html><body style='font-family:Segoe UI,Arial;padding:30px;'>
    <h2>Checkout cancelled</h2>
    <p>Your guild has been created, but access will stay locked until billing is completed or site admin manually activates it.</p>
    <p><a href='/'>Back to LM Guild Manager</a></p>
    </body></html>
    """)


@app.get("/billing/portal")
def billing_portal(request: Request):
    guild_id = current_guild_id(request)
    if not guild_id:
        return RedirectResponse(url="/guild/login", status_code=302)
    conn = get_conn()
    guild = conn.execute("SELECT * FROM guilds WHERE id = ?", (guild_id,)).fetchone()
    conn.close()
    if not guild or not guild["stripe_customer_id"]:
        return HTMLResponse("<h2>No Stripe customer found for this guild.</h2>", status_code=404)
    portal = stripe.billing_portal.Session.create(
        customer=guild["stripe_customer_id"],
        return_url=f"{PUBLIC_BASE_URL}/"
    )
    return RedirectResponse(portal.url, status_code=303)


@app.post("/stripe/webhook")
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")
    try:
        event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except Exception:
        return HTMLResponse("Invalid webhook", status_code=400)

    event_type = event.get("type")
    obj = event.get("data", {}).get("object", {})
    event_id = event.get("id", "")

    conn = get_conn()
    c = conn.cursor()

    try:
        if event_type == "checkout.session.completed":
            guild_id = obj.get("metadata", {}).get("guild_id")
            subscription_id = obj.get("subscription") or ""
            customer_id = obj.get("customer") or ""
            plan_key = obj.get("metadata", {}).get("plan", "monthly")
            if guild_id:
                c.execute("""
                    UPDATE guilds
                    SET stripe_customer_id = ?, stripe_subscription_id = ?, stripe_plan = ?, subscription_status = 'trialing', updated_at = ?
                    WHERE id = ?
                """, (customer_id, subscription_id, plan_key, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), guild_id))
                record_payment_event(conn, guild_id, event_type, "trialing", None, "GBP", "Checkout completed; trial started", event_id, "", subscription_id)

        elif event_type in {"customer.subscription.created", "customer.subscription.updated"}:
            sub = obj
            guild_id = sub.get("metadata", {}).get("guild_id")
            if not guild_id and sub.get("customer"):
                row = c.execute("SELECT id FROM guilds WHERE stripe_customer_id = ?", (sub.get("customer"),)).fetchone()
                guild_id = row["id"] if row else None
            if guild_id:
                status = sub.get("status") or ""
                c.execute("""
                    UPDATE guilds
                    SET stripe_subscription_id = ?, subscription_status = ?, trial_ends_at = ?, current_period_end = ?, updated_at = ?
                    WHERE id = ?
                """, (
                    sub.get("id") or "",
                    status,
                    dt_from_unix(sub.get("trial_end")) or "",
                    dt_from_unix(sub.get("current_period_end")) or "",
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    guild_id,
                ))
                record_payment_event(conn, guild_id, event_type, status, None, "GBP", f"Subscription status changed to {status}", event_id, "", sub.get("id") or "")

        elif event_type == "customer.subscription.deleted":
            sub = obj
            guild_id = sub.get("metadata", {}).get("guild_id")
            if not guild_id and sub.get("customer"):
                row = c.execute("SELECT id FROM guilds WHERE stripe_customer_id = ?", (sub.get("customer"),)).fetchone()
                guild_id = row["id"] if row else None
            if guild_id:
                c.execute("UPDATE guilds SET subscription_status = 'canceled', updated_at = ? WHERE id = ?", (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), guild_id))
                record_payment_event(conn, guild_id, event_type, "canceled", None, "GBP", "Subscription cancelled", event_id, "", sub.get("id") or "")

        elif event_type in {"invoice.paid", "invoice.payment_succeeded", "invoice.payment_failed"}:
            invoice = obj
            customer_id = invoice.get("customer") or ""
            row = c.execute("SELECT id FROM guilds WHERE stripe_customer_id = ?", (customer_id,)).fetchone()
            if row:
                guild_id = row["id"]
                amount = invoice.get("amount_paid") if event_type != "invoice.payment_failed" else invoice.get("amount_due")
                currency = invoice.get("currency") or "gbp"
                invoice_id = invoice.get("id") or ""
                sub_id = invoice.get("subscription") or ""
                if event_type == "invoice.payment_failed":
                    c.execute("UPDATE guilds SET subscription_status = 'past_due', updated_at = ? WHERE id = ?", (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), guild_id))
                    record_payment_event(conn, guild_id, event_type, "past_due", amount, currency, "Payment failed", event_id, invoice_id, sub_id)
                else:
                    c.execute("""
                        UPDATE guilds
                        SET subscription_status = 'active', last_payment_at = ?, last_payment_amount = ?, last_payment_currency = ?, updated_at = ?
                        WHERE id = ?
                    """, (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), amount or 0, (currency or "").upper(), datetime.now().strftime("%Y-%m-%d %H:%M:%S"), guild_id))
                    record_payment_event(conn, guild_id, event_type, "active", amount, currency, "Payment received", event_id, invoice_id, sub_id)

        conn.commit()
    finally:
        conn.close()

    return {"status": "ok"}

'''
if "/stripe/webhook" not in text:
    if routes_marker not in text:
        raise SystemExit("Could not find guild logout marker to insert billing routes")
    text = text.replace(routes_marker, billing_routes + routes_marker, 1)

# -----------------------------------------------------------------------------
# 6) Site admin dashboard/edit routes + manual billing controls
# -----------------------------------------------------------------------------
old_site_dash = '''    rows = conn.execute("""
        SELECT g.*, COUNT(m.id) AS member_count
        FROM guilds g
        LEFT JOIN members m ON m.guild_id = g.id
        GROUP BY g.id
        ORDER BY g.created_at DESC
    """).fetchall()
    conn.close()
    return templates.TemplateResponse(request, "site_admin_dashboard.html", {"guilds": rows})
'''
new_site_dash = '''    rows = conn.execute("""
        SELECT g.*, COUNT(m.id) AS member_count
        FROM guilds g
        LEFT JOIN members m ON m.guild_id = g.id
        GROUP BY g.id
        ORDER BY g.created_at DESC
    """).fetchall()
    guilds = []
    for g in rows:
        d = dict(g)
        d["billing_status_label"] = billing_status_label(d.get("subscription_status"), d.get("manual_access"))
        d["plan_label"] = get_billing_plan(d.get("stripe_plan"))["label"]
        d["plan_price"] = get_billing_plan(d.get("stripe_plan"))["display_price"]
        guilds.append(d)
    conn.close()
    return templates.TemplateResponse(request, "site_admin_dashboard.html", {"guilds": guilds})
'''
if old_site_dash in text:
    text = text.replace(old_site_dash, new_site_dash, 1)

old_edit_page = '''    conn = get_conn()
    guild = conn.execute("SELECT * FROM guilds WHERE id = ?", (guild_id,)).fetchone()
    conn.close()
    if not guild:
        return HTMLResponse("<h2>Guild not found</h2>", status_code=404)
    return templates.TemplateResponse(request, "site_admin_edit_guild.html", {"guild": guild})
'''
new_edit_page = '''    conn = get_conn()
    guild = conn.execute("SELECT * FROM guilds WHERE id = ?", (guild_id,)).fetchone()
    payments = conn.execute("""
        SELECT * FROM guild_payment_events
        WHERE guild_id = ?
        ORDER BY created_at DESC, id DESC
        LIMIT 50
    """, (guild_id,)).fetchall()
    conn.close()
    if not guild:
        return HTMLResponse("<h2>Guild not found</h2>", status_code=404)
    gd = dict(guild)
    gd["billing_status_label"] = billing_status_label(gd.get("subscription_status"), gd.get("manual_access"))
    gd["plan_label"] = get_billing_plan(gd.get("stripe_plan"))["label"]
    gd["plan_price"] = get_billing_plan(gd.get("stripe_plan"))["display_price"]
    return templates.TemplateResponse(request, "site_admin_edit_guild.html", {"guild": gd, "payments": payments, "plans": BILLING_PLANS})
'''
if old_edit_page in text:
    text = text.replace(old_edit_page, new_edit_page, 1)

# Extend site admin edit post to accept billing email/plan/status
old_edit_sig = '''    email: str = Form(""),
    guild_password: str = Form(""),
    admin_password: str = Form(""),
    disabled_reason: str = Form("")
):'''
new_edit_sig = '''    email: str = Form(""),
    billing_email: str = Form(""),
    stripe_plan: str = Form("monthly"),
    subscription_status: str = Form(""),
    guild_password: str = Form(""),
    admin_password: str = Form(""),
    disabled_reason: str = Form("")
):'''
if old_edit_sig in text:
    text = text.replace(old_edit_sig, new_edit_sig, 1)

old_updates = '''    updates = ["email = ?", "disabled_reason = ?", "updated_at = ?"]
    values = [email.strip(), disabled_reason.strip(), datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
'''
new_updates = '''    plan = get_billing_plan(stripe_plan)
    updates = ["email = ?", "billing_email = ?", "stripe_plan = ?", "stripe_price_id = ?", "disabled_reason = ?", "updated_at = ?"]
    values = [email.strip(), (billing_email or email).strip(), stripe_plan, plan["price_id"], disabled_reason.strip(), datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
    if subscription_status.strip():
        updates.append("subscription_status = ?")
        values.append(subscription_status.strip())
'''
if old_updates in text:
    text = text.replace(old_updates, new_updates, 1)

manual_routes_marker = '@app.post("/site-admin/guild/{guild_id}/disable")\ndef site_admin_disable_guild'
manual_routes = r'''
@app.post("/site-admin/guild/{guild_id}/manual-activate")
def site_admin_manual_activate_guild(request: Request, guild_id: int, manual_access_reason: str = Form("Manual access granted by site admin")):
    if not is_site_admin(request):
        return RedirectResponse(url="/site-admin/login", status_code=302)
    conn = get_conn()
    conn.execute("""
        UPDATE guilds
        SET manual_access = 1,
            manual_access_reason = ?,
            subscription_status = 'manual_active',
            updated_at = ?
        WHERE id = ?
    """, (manual_access_reason.strip() or "Manual access granted by site admin", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), guild_id))
    record_payment_event(conn, guild_id, "manual.activate", "manual_active", None, "GBP", manual_access_reason.strip() or "Manual access granted by site admin")
    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/site-admin/guild/{guild_id}", status_code=302)


@app.post("/site-admin/guild/{guild_id}/manual-deactivate")
def site_admin_manual_deactivate_guild(request: Request, guild_id: int):
    if not is_site_admin(request):
        return RedirectResponse(url="/site-admin/login", status_code=302)
    conn = get_conn()
    conn.execute("""
        UPDATE guilds
        SET manual_access = 0,
            manual_access_reason = '',
            subscription_status = CASE WHEN stripe_subscription_id IS NULL OR TRIM(stripe_subscription_id) = '' THEN 'pending_billing' ELSE subscription_status END,
            updated_at = ?
        WHERE id = ?
    """, (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), guild_id))
    record_payment_event(conn, guild_id, "manual.deactivate", "pending_billing", None, "GBP", "Manual access removed by site admin")
    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/site-admin/guild/{guild_id}", status_code=302)


'''
if "manual-activate" not in text:
    if manual_routes_marker not in text:
        raise SystemExit("Could not find site-admin disable route marker")
    text = text.replace(manual_routes_marker, manual_routes + manual_routes_marker, 1)

MAIN.write_text(text, encoding="utf-8")

# -----------------------------------------------------------------------------
# 7) requirements.txt
# -----------------------------------------------------------------------------
if REQ.exists():
    req = REQ.read_text(encoding="utf-8")
    if "stripe" not in req.lower():
        REQ.write_text(req.rstrip() + "\nstripe\n", encoding="utf-8")

py_compile.compile(str(MAIN), doraise=True)
print("Stripe billing update applied successfully.")
print("Backup created:", backup.name)
