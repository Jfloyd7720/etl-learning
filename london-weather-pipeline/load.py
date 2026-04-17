
import sqlite3

def load(data):
    conn=sqlite3.connect('weather.db')
    cursor=conn.cursor()
    cursor.execute(
        '''
        CREATE table if not exists weather_readings
        (
            city text,     
            time  text PRIMARY KEY,
            temperature_c float,
            wind_speed_kmh float,
            precipitation_mm float)
        '''
        

        )

    for row in data:
        
        cursor.execute(  
                        ''' 
                    insert or ignore into weather_readings(
                        city ,
                        time ,
                        temperature_c ,
                        wind_speed_kmh ,
                        precipitation_mm)
                        values(?,?,?,?,?)
                    ''',( row['city'],row['time'],row['temperature_c'],row['wind_speed_kmh'],row['precipitation_mm'])

                )
    conn.commit()
    print(f"[LOAD] Loaded weather reading for {row['city']} at {row['time']}")
    conn.close()   

if __name__ == "__main__":
    from extract import extract
    from transform import transform
    raw = extract()
    clean = transform(raw)
    load(clean)
    print("Loaded successfully")