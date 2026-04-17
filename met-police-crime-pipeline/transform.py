def transform(rows):
    cleaned = []

    for row in rows:
        # Skip rows with no Crime ID
        if not row.get('Crime ID', '').strip():
            continue

        cleaned_row = {
            'crime_id': row['Crime ID'].strip(),
            'month': row['Month'].strip(),
            'longitude': row['Longitude'].strip() or None,
            'latitude': row['Latitude'].strip() or None,
            'location': row['Location'].strip(),
            'lsoa_code': row['LSOA code'].strip() or None,
            'lsoa_name': row['LSOA name'].strip() or None,
            'crime_type': row['Crime type'].strip().title(),
            'outcome': row['Last outcome category'].strip() or None,
        }

        cleaned.append(cleaned_row)

    print(f'[TRANSFORM] {len(rows)} in → {len(cleaned)} clean rows out')
    return cleaned

if __name__ == "__main__":
    from extract import extract
    raw = extract("raw_data.csv")
    clean = transform(raw)
    print(clean[0])