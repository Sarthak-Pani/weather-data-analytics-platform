import pandas as pd
import mysql.connector

# Extract
file_path = "weather_data.csv"

df = pd.read_csv(file_path)

df = df.dropna()

df['date'] = pd.to_datetime(df['date'], dayfirst=True)

print("\nAfter Cleaning and Date Conversion:")
print(df.dtypes)

# Load
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="your_password",
    database="weather_analytics"
)

cursor = connection.cursor()

for _, row in df.iterrows():
    cursor.execute("""
    INSERT INTO weather_raw (date, city, temperature, humidity, rainfall)
    VALUES (%s, %s, %s, %s, %s)
    """, (
        row['date'].date(),
        row['city'],
        float(row['temperature']),
        float(row['humidity']),
        float(row['rainfall'])
    ))

connection.commit()

print("\nData inserted successfully!")

cursor.close()
connection.close()