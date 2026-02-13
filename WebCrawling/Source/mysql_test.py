import os
from pathlib import Path
from dotenv import load_dotenv
import pymysql

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env", override=True)  # mysql_test.py와 같은 폴더의 .env

print(BASE_DIR)

try:
    conn = pymysql.connect(
        host=os.environ["MYSQL_HOST"],
        port=int(os.environ.get("MYSQL_PORT", 3306)),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        db=os.environ["MYSQL_DB"],
        charset="utf8mb4",
    )
    print("DB connect OK")
    conn.close()
except Exception as e:
    print(f"DB Connection Error: {e}")