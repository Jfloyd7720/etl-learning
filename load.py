import sqlite3

def load(rows, db_path="pipeline.db"):
    conn = sqlite3.connect(db_path)
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

    for row in rows:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO crimes
                (crime_id, month, longitude, latitude, location, lsoa_code, lsoa_name, crime_type, outcome)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['crime_id'],
                row['month'],
                row['longitude'],
                row['latitude'],
                row['location'],
                row['lsoa_code'],
                row['lsoa_name'],
                row['crime_type'],
                row['outcome'],
            ))
            if cursor.rowcount == 1:
                inserted += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"[LOAD] Error on row {row['crime_id']}: {e}")

    conn.commit()
    conn.close()
    print(f"[LOAD] {inserted} inserted, {skipped} skipped")

if __name__ == "__main__":
    from extract import extract
    from transform import transform
    raw = extract("raw_data.csv")
    clean = transform(raw)
    load(clean)