import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get('DATABASE_URL')

conn = psycopg2.connect(DATABASE_URL)
print("Connected successfully")
conn.close()