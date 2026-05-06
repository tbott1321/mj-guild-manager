from pathlib import Path
import py_compile

MAIN = Path("main.py")
if not MAIN.exists():
    raise FileNotFoundError("main.py not found. Run this script from your MJ Guild Manager project folder.")

text = MAIN.read_text(encoding="utf-8")
backup = MAIN.with_suffix(".py.backup_stripe_webhook_get_fix")
backup.write_text(text, encoding="utf-8")

old = '    data = event["data"]["object"]\n'
new = '''    data = event["data"]["object"]
    # Stripe returns StripeObject instances, not normal dicts. Convert once so .get() works safely.
    if hasattr(data, "to_dict_recursive"):
        data = data.to_dict_recursive()
'''

if old not in text:
    raise RuntimeError('Could not find: data = event["data"]["object"]. Your webhook may already be patched or differs from expected.')

text = text.replace(old, new, 1)

MAIN.write_text(text, encoding="utf-8")
py_compile.compile(str(MAIN), doraise=True)

print("Stripe webhook .get() fix applied successfully.")
print(f"Backup created: {backup}")
