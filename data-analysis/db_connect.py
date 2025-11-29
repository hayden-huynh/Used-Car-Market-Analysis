import os
from dotenv import load_dotenv
from urllib.parse import quote_plus


def get_connection_url():
    # Load .env file from parent directory
    load_dotenv(os.path.join(os.path.dirname(os.getcwd()), ".env"))

    # Read credentials
    DB_USER = os.getenv("POSTGRES_USER")
    DB_PASS = quote_plus(os.getenv("POSTGRES_PWD"))

    return f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@localhost:5432/used_cars"
