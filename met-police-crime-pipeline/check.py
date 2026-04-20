import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set. Create a .env file.")

conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM crimes")
print(cursor.fetchone())
conn.close()