from pathlib import Path
import py_compile

MAIN = Path("main.py")
GUILD_REQ = Path("templates/guild_requirements.html")

main = MAIN.read_text(encoding="utf-8")
MAIN.write_text(main, encoding="utf-8")
Path("main.py.backup_auto_watchlist").write_text(main, encoding="utf-8")

if GUILD_REQ.exists():
    guild_req_old = GUILD_REQ.read_text(encoding="utf-8")
    Path("templates/guild_requirements.html.backup_auto_watchlist").write_text(guild_req_old, encoding="utf-8")

# 1. Add extra settings to get_guild_settings
main = main.replace(
'''    return {
        "min_mana": get_value("min_mana", 1),
        "min_sigils": get_value("min_sigils", 80)
    }
''',
'''    return {
        "min_mana": get_value("min_mana", 1),
        "min_sigils": get_value("min_sigils", 80),
        "report_fail_threshold": get_value("report_fail_threshold", 2),
        "auto_watch_requirements": get_value("auto_watch_requirements", 1)
    }
'''
)

# 2. Add helper function before guild requirements page
marker = '@app.get("/guild-requirements", response_class=HTMLResponse)'
helper = r'''
def get_auto_watchlist_candidates(conn):
    c = conn.cursor()
    settings = get_guild_settings(conn)

    c.execute("""
        SELECT * FROM members
        WHERE COALESCE(watchlist_flag, 0) = 0
        AND (
            COALESCE(mana, 0) < ?
            OR COALESCE(sigils, 0) < ?
        )
        ORDER BY LOWER(name)
    """, (settings["min_mana"], settings["min_sigils"]))
    requirement_failures = c.fetchall()

    c.execute("""
        SELECT m.*, COUNT(krr.id) AS fail_count
        FROM members m
        JOIN kill_report_rows krr ON krr.igg_id = m.igg_id
        WHERE COALESCE(m.watchlist_flag, 0) = 0
        AND krr.overall_pass = 0
        GROUP BY m.igg_id
        HAVING COUNT(krr.id) >= ?
        ORDER BY fail_count DESC, LOWER(m.name)
    """, (settings["report_fail_threshold"],))
    kill_failures = c.fetchall()

    c.execute("""
        SELECT m.*, COUNT(gfrr.id) AS fail_count
        FROM members m
        JOIN guild_fest_report_rows gfrr ON LOWER(gfrr.player_name) = LOWER(m.name)
        WHERE COALESCE(m.watchlist_flag, 0) = 0
        AND gfrr.passed = 0
        GROUP BY m.igg_id
        HAVING COUNT(gfrr.id) >= ?
        ORDER BY fail_count DESC, LOWER(m.name)
    """, (settings["report_fail_threshold"],))
    guild_fest_failures = c.fetchall()

    return {
        "requirement_failures": requirement_failures,
        "kill_failures": kill_failures,
        "guild_fest_failures": guild_fest_failures
    }


'''
if "def get_auto_watchlist_candidates" not in main:
    main = main.replace(marker, helper + marker)

# 3. Add auto-watchlist data into guild_requirements route response
main = main.replace(
'''    conn.close()

    return templates.TemplateResponse(request, "guild_requirements.html", {
        "settings": settings,
        "low_mana": low_mana,
        "low_sigils": low_sigils,
        "both": both,
        "is_admin": True
    })
''',
'''    auto_watchlist = get_auto_watchlist_candidates(conn)

    conn.close()

    return templates.TemplateResponse(request, "guild_requirements.html", {
        "settings": settings,
        "low_mana": low_mana,
        "low_sigils": low_sigils,
        "both": both,
        "auto_watchlist": auto_watchlist,
        "is_admin": True
    })
'''
)

# 4. Update settings route signature
main = main.replace(
'''def update_guild_requirements(
    request: Request,
    min_mana: int = Form(...),
    min_sigils: int = Form(...)
):
''',
'''def update_guild_requirements(
    request: Request,
    min_mana: int = Form(...),
    min_sigils: int = Form(...),
    report_fail_threshold: int = Form(2),
    auto_watch_requirements: str | None = Form(default=None)
):
'''
)

# 5. Add saving new settings
main = main.replace(
'''    min_mana = max(0, int(min_mana))
    min_sigils = max(0, int(min_sigils))

    conn = get_conn()
''',
'''    min_mana = max(0, int(min_mana))
    min_sigils = max(0, int(min_sigils))
    report_fail_threshold = max(1, int(report_fail_threshold))
    auto_watch_requirements_value = 1 if auto_watch_requirements else 0

    conn = get_conn()
'''
)

main = main.replace(
'''    c.execute("""
        INSERT OR REPLACE INTO guild_settings (setting_key, setting_value)
        VALUES (?, ?)
    """, ("min_sigils", str(min_sigils)))

    conn.commit()
''',
'''    c.execute("""
        INSERT OR REPLACE INTO guild_settings (setting_key, setting_value)
        VALUES (?, ?)
    """, ("min_sigils", str(min_sigils)))

    c.execute("""
        INSERT OR REPLACE INTO guild_settings (setting_key, setting_value)
        VALUES (?, ?)
    """, ("report_fail_threshold", str(report_fail_threshold)))

    c.execute("""
        INSERT OR REPLACE INTO guild_settings (setting_key, setting_value)
        VALUES (?, ?)
    """, ("auto_watch_requirements", str(auto_watch_requirements_value)))

    conn.commit()
'''
)

MAIN.write_text(main, encoding="utf-8")

# 6. Replace guild requirements page with upgraded version
guild_requirements_html = r'''<!DOCTYPE html>
<html>
<head>
    <title>Guild Requirements</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">

    <style>
        body {
            font-family: Segoe UI, sans-serif;
            background: #f5f7fb;
            margin: 0;
            padding: 0;
            color: #111827;
        }

        .topbar {
            background: linear-gradient(135deg, #1e293b, #312e81);
            color: white;
            padding: 20px;
        }

        .topbar h1 {
            margin: 0;
            font-size: 28px;
        }

        .top-actions {
            margin-top: 15px;
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
        }

        .btn {
            background: white;
            color: #312e81;
            padding: 10px 16px;
            border-radius: 8px;
            text-decoration: none;
            font-weight: bold;
            font-size: 14px;
            border: none;
            cursor: pointer;
        }

        .container {
            padding: 20px;
        }

        .card {
            background: white;
            padding: 20px;
            border-radius: 12px;
            margin-bottom: 20px;
            box-shadow: 0px 3px 10px rgba(0,0,0,0.08);
        }

        .settings-grid {
            display: flex;
            flex-wrap: wrap;
            gap: 12px;
            align-items: end;
        }

        label {
            font-weight: bold;
            font-size: 13px;
            color: #475569;
            display: block;
            margin-bottom: 6px;
        }

        input {
            padding: 10px;
            border: 1px solid #ddd;
            border-radius: 8px;
            min-width: 140px;
        }

        .checkbox-row {
            display: flex;
            align-items: center;
            gap: 8px;
            padding-bottom: 10px;
        }

        .checkbox-row input {
            min-width: auto;
        }

        .save-btn {
            background: #312e81;
            color: white;
            padding: 11px 18px;
            border-radius: 8px;
            border: none;
            font-weight: bold;
            cursor: pointer;
        }

        .summary-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 14px;
        }

        .summary-card {
            background: #f8fafc;
            padding: 16px;
            border-radius: 12px;
        }

        .summary-label {
            color: #6b7280;
            font-size: 13px;
            margin-bottom: 6px;
        }

        .summary-value {
            font-size: 26px;
            font-weight: bold;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 12px;
            overflow: hidden;
        }

        th {
            background: #1e293b;
            color: white;
            padding: 12px;
            text-align: left;
        }

        td {
            padding: 12px;
            border-bottom: 1px solid #eee;
        }

        tr:hover {
            background: #f8fafc;
        }

        .pill {
            display: inline-block;
            background: #fee2e2;
            color: #991b1b;
            padding: 4px 8px;
            border-radius: 999px;
            font-size: 12px;
            font-weight: bold;
        }

        .pill-warning {
            background: #fef3c7;
            color: #92400e;
        }

        .section-title {
            margin-top: 0;
            color: #1e293b;
        }

        .mini-btn {
            background: #312e81;
            color: white;
            padding: 8px 12px;
            border-radius: 8px;
            border: none;
            font-size: 13px;
            font-weight: bold;
            cursor: pointer;
        }

        @media (max-width: 768px) {
            .summary-grid {
                grid-template-columns: 1fr;
            }

            .settings-grid {
                flex-direction: column;
                align-items: stretch;
            }

            .btn,
            .save-btn {
                width: 100%;
                text-align: center;
            }

            table {
                font-size: 13px;
            }
        }
    </style>
</head>

<body>

<div class="topbar">
    <h1>Guild Requirements</h1>

    <div class="top-actions">
        <a href="/" class="btn">Back to Dashboard</a>
    </div>
</div>

<div class="container">

    <div class="card">
        <h2 class="section-title">Requirement Settings</h2>

        <form method="post" action="/guild-requirements/update">
            <div class="settings-grid">
                <div>
                    <label>Minimum Mana</label>
                    <input type="number" name="min_mana" min="0" step="1" value="{{ settings.min_mana }}">
                </div>

                <div>
                    <label>Minimum Sigils</label>
                    <input type="number" name="min_sigils" min="0" step="1" value="{{ settings.min_sigils }}">
                </div>

                <div>
                    <label>Report Fail Threshold</label>
                    <input type="number" name="report_fail_threshold" min="1" step="1" value="{{ settings.report_fail_threshold }}">
                </div>

                <div class="checkbox-row">
                    <input type="checkbox" name="auto_watch_requirements" value="1" {% if settings.auto_watch_requirements %}checked{% endif %}>
                    <span>Include requirement failures in auto-watchlist</span>
                </div>

                <button class="save-btn" type="submit">Save Requirements</button>
            </div>
        </form>
    </div>

    <div class="card">
        <h2 class="section-title">Requirement Summary</h2>

        <div class="summary-grid">
            <div class="summary-card">
                <div class="summary-label">Below Mana Requirement</div>
                <div class="summary-value">{{ low_mana|length }}</div>
            </div>

            <div class="summary-card">
                <div class="summary-label">Below Sigil Requirement</div>
                <div class="summary-value">{{ low_sigils|length }}</div>
            </div>

            <div class="summary-card">
                <div class="summary-label">Failing Both</div>
                <div class="summary-value">{{ both|length }}</div>
            </div>
        </div>
    </div>

    <div class="card">
        <h2 class="section-title">Auto Watchlist Candidates</h2>

        <h3>Requirement Failures</h3>
        <table>
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Mana</th>
                    <th>Sigils</th>
                    <th>Reason</th>
                    <th>Action</th>
                </tr>
            </thead>
            <tbody>
                {% for m in auto_watchlist.requirement_failures %}
                <tr>
                    <td>{{ m.name }}</td>
                    <td>{{ m.mana }}</td>
                    <td>{{ m.sigils }}</td>
                    <td><span class="pill-warning pill">Below Requirements</span></td>
                    <td>
                        <form action="/member/{{ m.igg_id }}/watchlist" method="post">
                            <input type="hidden" name="watchlist_flag" value="1">
                            <button class="mini-btn" type="submit">Add</button>
                        </form>
                    </td>
                </tr>
                {% endfor %}
            </tbody>
        </table>

        <h3>Kill Report Failures</h3>
        <table>
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Fail Count</th>
                    <th>Threshold</th>
                    <th>Action</th>
                </tr>
            </thead>
            <tbody>
                {% for m in auto_watchlist.kill_failures %}
                <tr>
                    <td>{{ m.name }}</td>
                    <td>{{ m.fail_count }}</td>
                    <td>{{ settings.report_fail_threshold }}</td>
                    <td>
                        <form action="/member/{{ m.igg_id }}/watchlist" method="post">
                            <input type="hidden" name="watchlist_flag" value="1">
                            <button class="mini-btn" type="submit">Add</button>
                        </form>
                    </td>
                </tr>
                {% endfor %}
            </tbody>
        </table>

        <h3>Guild Fest Failures</h3>
        <table>
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Fail Count</th>
                    <th>Threshold</th>
                    <th>Action</th>
                </tr>
            </thead>
            <tbody>
                {% for m in auto_watchlist.guild_fest_failures %}
                <tr>
                    <td>{{ m.name }}</td>
                    <td>{{ m.fail_count }}</td>
                    <td>{{ settings.report_fail_threshold }}</td>
                    <td>
                        <form action="/member/{{ m.igg_id }}/watchlist" method="post">
                            <input type="hidden" name="watchlist_flag" value="1">
                            <button class="mini-btn" type="submit">Add</button>
                        </form>
                    </td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>

</div>

</body>
</html>
'''

GUILD_REQ.write_text(guild_requirements_html, encoding="utf-8")

py_compile.compile("main.py", doraise=True)

print("Auto Watchlist update applied successfully.")
print("Backups created:")
print(" - main.py.backup_auto_watchlist")
print(" - templates/guild_requirements.html.backup_auto_watchlist")