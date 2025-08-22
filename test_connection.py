import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()  # reads .env in this folder
url = os.getenv("DATABASE_URL")
print("DATABASE_URL loaded:", bool(url))
print("URL:", url or "(missing)")

engine = create_engine(url, pool_pre_ping=True)

try:
    with engine.begin() as c:
        row = c.execute(text("select current_database(), version()")).fetchone()
        print("Connected to:", row[0])
        print(row[1])
except Exception as e:
    print("Connection failed:", repr(e))
