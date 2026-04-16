import pandas as pd
from transform import transform

def classify_severity(crime_type):
    high = ['Violence And Sexual Offences', 'Robbery', 'Possession Of Weapons']
    medium = ['Burglary', 'Drugs', 'Criminal Damage And Arson']
    if crime_type in high:
        return 'High'
    elif crime_type in medium:
        return 'Medium'
    else:
        return 'Low'

def transform_pandas(filepath):
    # Load
    df = pd.read_csv(filepath)
    print(f"[EXTRACT] Loaded {df.shape[0]} rows, {df.shape[1]} columns")

    # Drop useless columns
    df = df.drop(columns=['Context', 'Reported by', 'Falls within'])

    # Rename to snake_case
    df = df.rename(columns={
        'Crime ID': 'crime_id',
        'Month': 'month',
        'Longitude': 'longitude',
        'Latitude': 'latitude',
        'Location': 'location',
        'LSOA code': 'lsoa_code',
        'LSOA name': 'lsoa_name',
        'Crime type': 'crime_type',
        'Last outcome category': 'outcome'
    })

    # Drop rows with no crime_id
    df = df.dropna(subset=['crime_id'])

    # Normalise text
    df['crime_type'] = df['crime_type'].str.strip().str.title()
    df['outcome'] = df['outcome'].str.strip()
    df['location'] = df['location'].str.strip()

    # Deduplicate
    before = len(df)
    df = df.drop_duplicates(subset=['crime_id'])
    after = len(df)
    # print(f"[TRANSFORM] Removed {before - after} duplicates")
    # print(f"[TRANSFORM] {len(df)} clean rows ready")
    df['severity'] = df['crime_type'].apply(classify_severity)
    print(df['severity'].value_counts())
    return df

    return df

def compare_transforms(func1, func2, filepath):
    from extract import extract
    raw = extract(filepath)
    result1 = len(func1(raw))
    result2 = len(func2(filepath))
    # print(f"Old transform: {result1} rows (deduplication in load step)")
    # print(f"New transform: {result2} rows (deduplication in transform step)")
    # print(f"Difference: {result1 - result2} duplicates handled earlier in new version")

if __name__ == "__main__":
    df = transform_pandas("raw_data.csv")
    print(df.groupby(['crime_type', 'lsoa_name'])['crime_id'].count().groupby('crime_type').mean().round(2))
    # compare_transforms(transform,transform_pandas,"raw_data.csv")
    # print(df.head())


