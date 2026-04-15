import csv
def extract(filepath):
    rows =[]
    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
        print(f'[EXTRACT] Loaded {len(rows)} rows')
        return rows

if __name__ == "__main__":
    data = extract("raw_data.csv")
    print(data[0])
    