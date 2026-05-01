$ErrorActionPreference = "Stop"

Copy-Item "main.py" "main.py.backup_guild_requirements"
Copy-Item "templates/dashboard.html" "templates/dashboard.html.backup_guild_requirements"

$main = Get-Content "main.py" -Raw
$dash = Get-Content "templates/dashboard.html" -Raw

# Add guild_settings to TABLES
if ($main -notmatch '"guild_settings"') {
   $main = $main.Replace(
    '"guild_fest_report_rows",',
    '"guild_fest_report_rows",' + "`r`n    `"guild_settings`","
)
}

# Add guild_settings table
if ($main -notmatch 'CREATE TABLE IF NOT EXISTS guild_settings') {
    $insertAfter = @'
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
'@

    $guildSettingsTable = $insertAfter + @'

    c.execute("""
        CREATE TABLE IF NOT EXISTS guild_settings (
            setting_key TEXT PRIMARY KEY,
            setting_value TEXT
        )
    """)
'@

    $main = $main.Replace($insertAfter, $guildSettingsTable)
}

# Add get_guild_settings helper
if ($main -notmatch 'def get_guild_settings') {
    $marker = "def get_dashboard_insights(conn):"
    $helper = @'
def get_guild_settings(conn):
    c = conn.cursor()

    def get_value(key, default):
        c.execute("SELECT setting_value FROM guild_settings WHERE setting_key = ?", (key,))
        row = c.fetchone()
        try:
            return int(row["setting_value"]) if row else default
        except (TypeError, ValueError):
            return default

    return {
        "min_mana": get_value("min_mana", 1),
        "min_sigils": get_value("min_sigils", 80)
    }


'@
    $main = $main.Replace($marker, $helper + $marker)
}

# Replace fixed dashboard insight thresholds
$old = @'
    c.execute("SELECT COUNT(*) AS cnt FROM members WHERE COALESCE(mana, 0) < 1")
    low_mana_count = c.fetchone()["cnt"] or 0

    c.execute("SELECT COUNT(*) AS cnt FROM members WHERE COALESCE(sigils, 0) < 80")
    low_sigils_count = c.fetchone()["cnt"] or 0
'@

$new = @'
    settings = get_guild_settings(conn)

    c.execute("SELECT COUNT(*) AS cnt FROM members WHERE COALESCE(mana, 0) < ?", (settings["min_mana"],))
    low_mana_count = c.fetchone()["cnt"] or 0

    c.execute("SELECT COUNT(*) AS cnt FROM members WHERE COALESCE(sigils, 0) < ?", (settings["min_sigils"],))
    low_sigils_count = c.fetchone()["cnt"] or 0
'@

$main = $main.Replace($old, $new)

# Add routes
if ($main -notmatch '@app.get\("/guild-requirements"') {
    $routeMarker = '@app.get("/pending-members", response_class=HTMLResponse)'
    $routes = @'
@app.get("/guild-requirements", response_class=HTMLResponse)
def guild_requirements_page(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    conn = get_conn()
    c = conn.cursor()
    settings = get_guild_settings(conn)

    c.execute("""
        SELECT * FROM members
        WHERE COALESCE(mana, 0) < ?
        ORDER BY COALESCE(mana, 0) ASC, LOWER(name) ASC
    """, (settings["min_mana"],))
    low_mana = c.fetchall()

    c.execute("""
        SELECT * FROM members
        WHERE COALESCE(sigils, 0) < ?
        ORDER BY COALESCE(sigils, 0) ASC, LOWER(name) ASC
    """, (settings["min_sigils"],))
    low_sigils = c.fetchall()

    c.execute("""
        SELECT * FROM members
        WHERE COALESCE(mana, 0) < ?
        AND COALESCE(sigils, 0) < ?
        ORDER BY LOWER(name) ASC
    """, (settings["min_mana"], settings["min_sigils"]))
    both = c.fetchall()

    conn.close()

    return templates.TemplateResponse(request, "guild_requirements.html", {
        "settings": settings,
        "low_mana": low_mana,
        "low_sigils": low_sigils,
        "both": both,
        "is_admin": True
    })


@app.post("/guild-requirements/update")
def update_guild_requirements(
    request: Request,
    min_mana: int = Form(...),
    min_sigils: int = Form(...)
):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    min_mana = max(0, int(min_mana))
    min_sigils = max(0, int(min_sigils))

    conn = get_conn()
    c = conn.cursor()

    c.execute("""
        INSERT OR REPLACE INTO guild_settings (setting_key, setting_value)
        VALUES (?, ?)
    """, ("min_mana", str(min_mana)))

    c.execute("""
        INSERT OR REPLACE INTO guild_settings (setting_key, setting_value)
        VALUES (?, ?)
    """, ("min_sigils", str(min_sigils)))

    conn.commit()
    conn.close()

    return RedirectResponse(url="/guild-requirements", status_code=302)


'@
    $main = $main.Replace($routeMarker, $routes + $routeMarker)
}

# Add dashboard button
if ($dash -notmatch '/guild-requirements') {
    $dash = $dash.Replace(
    '<a href="/former-members" class="btn">Former Members</a>',
    '<a href="/former-members" class="btn">Former Members</a>' + "`r`n            <a href=`"/guild-requirements`" class=`"btn`">Guild Requirements</a>"
)
}

# Update labels on dashboard insights
$dash = $dash.Replace('Low Mana (&lt;1)', 'Low Mana')
$dash = $dash.Replace('Low Sigils (&lt;80)', 'Low Sigils')

Set-Content "main.py" $main -Encoding UTF8
Set-Content "templates/dashboard.html" $dash -Encoding UTF8

# Create guild requirements page
$guildRequirements = @'
<!DOCTYPE html>
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

        .section-title {
            margin-top: 0;
            color: #1e293b;
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
        <h2 class="section-title">Players Below Mana Minimum</h2>

        <table>
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Rank</th>
                    <th>Mana</th>
                    <th>Sigils</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
                {% for m in low_mana %}
                <tr onclick="window.location='/member/{{ m.igg_id }}'" style="cursor:pointer;">
                    <td>{{ m.name }}</td>
                    <td>{{ m.rank }}</td>
                    <td>{{ m.mana }}</td>
                    <td>{{ m.sigils }}</td>
                    <td><span class="pill">Needs Mana</span></td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>

    <div class="card">
        <h2 class="section-title">Players Below Sigil Minimum</h2>

        <table>
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Rank</th>
                    <th>Mana</th>
                    <th>Sigils</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
                {% for m in low_sigils %}
                <tr onclick="window.location='/member/{{ m.igg_id }}'" style="cursor:pointer;">
                    <td>{{ m.name }}</td>
                    <td>{{ m.rank }}</td>
                    <td>{{ m.mana }}</td>
                    <td>{{ m.sigils }}</td>
                    <td><span class="pill">Needs Sigils</span></td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>

    <div class="card">
        <h2 class="section-title">Players Failing Both Requirements</h2>

        <table>
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Rank</th>
                    <th>Mana</th>
                    <th>Sigils</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
                {% for m in both %}
                <tr onclick="window.location='/member/{{ m.igg_id }}'" style="cursor:pointer;">
                    <td>{{ m.name }}</td>
                    <td>{{ m.rank }}</td>
                    <td>{{ m.mana }}</td>
                    <td>{{ m.sigils }}</td>
                    <td><span class="pill">Failing Both</span></td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>

</div>

</body>
</html>
'@

Set-Content "templates/guild_requirements.html" $guildRequirements -Encoding UTF8

python -m py_compile main.py

Write-Host "Guild Requirements update applied successfully."
Write-Host "Backups created:"
Write-Host " - main.py.backup_guild_requirements"
Write-Host " - templates/dashboard.html.backup_guild_requirements"