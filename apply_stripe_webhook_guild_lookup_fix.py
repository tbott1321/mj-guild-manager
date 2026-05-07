from pathlib import Path
import py_compile

MAIN = Path("main.py")
if not MAIN.exists():
    raise FileNotFoundError("main.py not found. Put this script in the same folder as main.py and run it again.")

text = MAIN.read_text(encoding="utf-8")
backup = MAIN.with_suffix(".py.backup_webhook_guild_fallback")
backup.write_text(text, encoding="utf-8")

needle = """    event_data = stripe_safe_dict(event_dict.get("data", {}))
    data = stripe_safe_dict(event_data.get("object", {}))

    conn = get_conn()
"""

replacement = """    event_data = stripe_safe_dict(event_dict.get("data", {}))
    data = stripe_safe_dict(event_data.get("object", {}))

    conn = get_conn()

    def get_event_guild_id(obj):
        obj = stripe_safe_dict(obj)

        meta = obj.get("metadata") or {}
        if hasattr(meta, "to_dict_recursive"):
            meta = meta.to_dict_recursive()
        if isinstance(meta, dict):
            gid = meta.get("guild_id")
            if gid:
                return int(gid)

        customer_id = obj.get("customer")
        if customer_id:
            row = conn.execute(
                "SELECT id FROM guilds WHERE stripe_customer_id = ?",
                (customer_id,)
            ).fetchone()
            if row:
                return int(row["id"])

        subscription_id = obj.get("subscription") or obj.get("id")
        if subscription_id:
            row = conn.execute(
                "SELECT id FROM guilds WHERE stripe_subscription_id = ?",
                (subscription_id,)
            ).fetchone()
            if row:
                return int(row["id"])

        return None
"""

if needle not in text:
    raise RuntimeError("Could not find webhook data conversion block. Check current stripe_webhook structure.")

text = text.replace(needle, replacement, 1)

text = text.replace(
    'guild_id = data.get("metadata", {}).get("guild_id")',
    'guild_id = get_event_guild_id(data)'
)
text = text.replace(
    'guild_id = sub.get("metadata", {}).get("guild_id")',
    'guild_id = get_event_guild_id(sub)'
)

MAIN.write_text(text, encoding="utf-8")
py_compile.compile(str(MAIN), doraise=True)

print("Stripe webhook guild lookup fallback applied successfully.")
print(f"Backup created: {backup}")
