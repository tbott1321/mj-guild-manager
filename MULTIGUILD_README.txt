LM Guild Manager - Option B Multiguild Update

What changed:
- Landing page branded as LM Guild Manager.
- Guild login/create system.
- Guild tags are case-sensitive and exactly 3 printable non-space characters.
- Single shared database with guild_id isolation.
- Existing data is migrated into guild tag M/J.
- M/J default passwords are admin123 unless overridden with environment variables:
  DEFAULT_MJ_GUILD_PASSWORD
  DEFAULT_MJ_ADMIN_PASSWORD
  DEFAULT_MJ_EMAIL
- Site admin login is at /site-admin/login.
- Site admin password defaults to siteadmin123 unless overridden with SITE_ADMIN_PASSWORD.

Recommended Render environment variables:
SESSION_SECRET = a long random secret
SITE_ADMIN_PASSWORD = your private site admin password
DEFAULT_MJ_GUILD_PASSWORD = temporary M/J guild password
DEFAULT_MJ_ADMIN_PASSWORD = temporary M/J admin password
DEFAULT_MJ_EMAIL = your support/owner email

Important:
- Test locally before pushing.
- The legacy full database restore route is disabled in multiguild mode. Use Excel export/import for guild-scoped data.
