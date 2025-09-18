import os

# куда Superset пишет свои метаданные (не ваши данные, а служебные)
SQLALCHEMY_DATABASE_URI = os.getenv(
    "SUPERSET_DATABASE_URI",
    "sqlite:////app/superset_home/superset.db"  # fallback для dev
)

# ключ для сессий/CSRF
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "dev")

# кэш/флаги (минимум)
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}
