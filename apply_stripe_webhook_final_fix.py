from pathlib import Path
import re
import py_compile

MAIN = Path("main.py")
if not MAIN.exists():
    raise FileNotFoundError("main.py not found. Run this script from your MJ Guild Manager project folder.")

text = MAIN.read_text(encoding="utf-8")
backup = MAIN.with_suffix(".py.backup_stripe_webhook_final")
backup.write_text(text, encoding="utf-8")

# Replace the event/data setup in stripe_webhook so StripeObject objects are converted to plain dicts.
pattern = r'''    event_type = event\["type"\]\s*
    data = event\["data"\]\["object"\]\s*

    (?:# Stripe returns StripeObject instances, not normal dicts\. Convert once so \.get\(\) works safely\.\s*
    if hasattr\(data, "to_dict_recursive"\):\s*
        data = data\.to_dict_recursive\(\)\s*)?
    conn = get_conn\(\)
'''

replacement = '''    # Stripe returns StripeObject instances, not normal dicts.
    # Convert event and nested objects to plain dicts before using .get().
    if hasattr(event, "to_dict_recursive"):
        event = event.to_dict_recursive()
    elif not isinstance(event, dict):
        event = dict(event)

    event_type = event.get("type", "")
    event_id = event.get("id", "")
    data = event.get("data", {}).get("object", {})

    def stripe_safe_dict(obj):
        if hasattr(obj, "to_dict_recursive"):
            return obj.to_dict_recursive()
        if isinstance(obj, dict):
            return obj
        try:
            return dict(obj)
        except Exception:
            return {}

    data = stripe_safe_dict(data)
    conn = get_conn()
'''

text, count = re.subn(pattern, replacement, text, count=1)
if count != 1:
    raise RuntimeError("Could not patch the event/data setup in stripe_webhook. Please check the current webhook code.")

# Convert nested StripeObjects used in the subscription and invoice branches.
text = text.replace("sub = data", "sub = stripe_safe_dict(data)")
text = text.replace("invoice = data", "invoice = stripe_safe_dict(data)")

# The webhook now stores event_id once as a normal string.
text = text.replace('event.get("id", "")', "event_id")

MAIN.write_text(text, encoding="utf-8")
py_compile.compile(str(MAIN), doraise=True)

print("Stripe webhook final object-conversion fix applied successfully.")
print(f"Backup created: {backup}")
