import sqlite3
import pandas as pd

conn = sqlite3.connect("pipeline.db")

print("\n--- Task 1: Crime types ranked by frequency ---")
q1 = """
SELECT 
    crime_type,
    COUNT(*) as total,
    RANK() OVER (ORDER BY COUNT(*) DESC) as rank
FROM crimes
GROUP BY crime_type
"""
print(pd.read_sql_query(q1, conn).to_string())

print("\n--- Task 2: Percentage of total crimes ---")
q2 = """
WITH counts AS (
    SELECT crime_type, COUNT(*) as total_crimes
    FROM crimes
    GROUP BY crime_type
)
SELECT 
    crime_type,
    total_crimes,
    ROUND(100.0 * total_crimes / SUM(total_crimes) OVER(), 2) as pct_of_total
FROM counts
ORDER BY pct_of_total DESC
"""
print(pd.read_sql_query(q2, conn).to_string())

print("\n--- Task 3: Top 3 outcomes per crime type ---")
q3 = """
WITH counts AS (
    SELECT 
        crime_type,
        outcome,
        COUNT(*) as total,
        DENSE_RANK() OVER (PARTITION BY crime_type ORDER BY COUNT(*) DESC) as rank
    FROM crimes
    GROUP BY crime_type, outcome
)
SELECT * FROM counts
WHERE rank <= 3
"""
print(pd.read_sql_query(q3, conn).to_string())

print("\n--- Task 4: Crimes with over 50 pct unsolved rate ---")
q4 = """
SELECT 
    crime_type,
    COUNT(*) as total,
    SUM(CASE WHEN outcome = 'Investigation complete; no suspect identified' 
        THEN 1 ELSE 0 END) as unsolved,
    ROUND(100.0 * SUM(CASE WHEN outcome = 'Investigation complete; no suspect identified' 
        THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_unsolved
FROM crimes
GROUP BY crime_type
HAVING unsolved >= total/2
ORDER BY pct_unsolved DESC;
"""
print(pd.read_sql_query(q4, conn).to_string())

conn.close()