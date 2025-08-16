import os
SQLALCHEMY_DATABASE_URI = os.getenv("SUPERSET_DATABASE_URI", "sqlite:////app/superset_home/superset.db")
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "dev")
FEATURE_FLAGS = {"ENABLE_TEMPLATE_PROCESSING": True}
