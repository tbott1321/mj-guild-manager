from pathlib import Path
import py_compile

MAIN = Path("main.py")
if not MAIN.exists():
    raise FileNotFoundError("main.py not found. Put this script in the same folder as main.py and run again.")

text = MAIN.read_text(encoding="utf-8")
backup = MAIN.with_suffix(".py.backup_fix_event_dict")
backup.write_text(text, encoding="utf-8")

old = """    elif not isinstance(event, dict):
        event = dict(event)
"""

new = """    elif hasattr(event, "_data"):
        event = event._data
    elif not isinstance(event, dict):
        event = {}
"""

if old not in text:
    raise RuntimeError("Could not find the bad event = dict(event) block. It may already be fixed or the webhook code is different.")

text = text.replace(old, new, 1)

MAIN.write_text(text, encoding="utf-8")
py_compile.compile(str(MAIN), doraise=True)

print("Fixed invalid Stripe event dict conversion.")
print(f"Backup created: {backup}")
