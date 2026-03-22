import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "superset-secret-change-in-prod")
# SQLite — avoids needing psycopg2 in the Superset image
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

WTF_CSRF_ENABLED = False
FEATURE_FLAGS = {"ENABLE_TEMPLATE_PROCESSING": True}

# Allow embedding in iframes for dashboard sharing
HTTP_HEADERS = {}
TALISMAN_ENABLED = False
