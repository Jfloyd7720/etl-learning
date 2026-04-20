import psycopg2
import os
from dotenv import load_dotenv
import psycopg2.extras

load_dotenv()

DATABASE_URL = os.environ.get('DATABASE_URL')

if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set. Create a .env file.")

def load(rows):
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS crimes (
            crime_id TEXT PRIMARY KEY,
            month TEXT,
            longitude TEXT,
            latitude TEXT,
            location TEXT,
            lsoa_code TEXT,
            lsoa_name TEXT,
            crime_type TEXT,
            outcome TEXT
        )
    """)

    inserted = 0
    skipped = 0

    psycopg2.extras.execute_values(
        cursor,
        """
        INSERT INTO crimes
        (crime_id, month, longitude, latitude, location, lsoa_code, lsoa_name, crime_type, outcome)
        VALUES %s
        ON CONFLICT (crime_id) DO NOTHING
        """,
        [(
            row['crime_id'],
            row['month'],
            row['longitude'],
            row['latitude'],
            row['location'],
            row['lsoa_code'],
            row['lsoa_name'],
            row['crime_type'],
            row['outcome'],
        ) for row in rows]
    )
    inserted = cursor.rowcount
    print(f"[LOAD] {inserted} rows processed")
    conn.commit()
    conn.close()

if __name__ == "__main__":
    from extract import extract
    from transform import transform
    raw = extract("raw_data.csv")
    clean = transform(raw)
    load(clean)