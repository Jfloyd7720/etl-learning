from extract import extract


def transform(data):
    
    transformed = []
    for i,j in enumerate(data['hourly']['time']):
                    transformed.append(
                    {'city':'london',
                    'time':data['hourly']['time'][i], 
                    'temperature_c':data['hourly']['temperature_2m'][i],
                    'wind_speed_kmh': data['hourly']['wind_speed_10m'][i],
                    'precipitation_mm': data['hourly']['precipitation'][i]})

    return transformed

if __name__ == "__main__":
    raw = extract()
    clean = transform(raw)
  