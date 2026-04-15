import sqlite3

conn = sqlite3.connect("pipeline.db")
cursor = conn.cursor()

print("\n--- Total crimes loaded ---")
cursor.execute("SELECT COUNT(*) FROM crimes")
print(cursor.fetchone()[0])

print("\n--- Top 5 crime types ---")
cursor.execute("""
    SELECT crime_type, COUNT(*) as total
    FROM crimes
    GROUP BY crime_type
    ORDER BY total DESC
    LIMIT 5
""")
for row in cursor.fetchall():
    print(row)

print("\n--- Top 5 outcomes ---")
cursor.execute("""
    SELECT outcome, COUNT(*) as total
    FROM crimes
    GROUP BY outcome
    ORDER BY total DESC
    LIMIT 5
""")
for row in cursor.fetchall():
    print(row)

print("\n--- Crimes with no location ---")
cursor.execute("SELECT COUNT(*) FROM crimes WHERE longitude IS NULL")
print(cursor.fetchone()[0])

conn.close()