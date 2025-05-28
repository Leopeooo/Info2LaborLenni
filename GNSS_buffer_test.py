import os
import time
import serial
import csv
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
import psycopg2

load_dotenv(dotenv_path=Path.cwd() / ".env")

SERIAL_PORT = os.getenv("SERIAL_PORT", "/dev/serial0")
BAUD_RATE    = int(os.getenv("BAUD_RATE", 9600))
BUFFER_FILE  = "buffer.csv"

DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# --------------------------------
# Hilfsfunktionen
# --------------------------------
def parse_gpgga(line):
    parts = line.split(',')
    if len(parts) < 10:
        return None
    lat = convert_to_decimal(parts[2], parts[3])
    lon = convert_to_decimal(parts[4], parts[5])
    alt = float(parts[9]) if parts[9] else None
    return (lat, lon, alt)

def parse_gprmc(line):
    parts = line.split(',')
    # Teile: [0]="$GPRMC", [7]=speed over ground in Knoten
    if len(parts) < 8 or parts[7]=='':
        return None
    speed_kn = float(parts[7])
    # Umrechnung Knoten → km/h
    return speed_kn * 1.852

def convert_to_decimal(raw, direction):
    if not raw or not direction:
        return None
    deg  = int(float(raw) / 100)
    minu = float(raw) - deg*100
    dec  = deg + minu/60
    if direction in ['S','W']:
        dec = -dec
    return dec

def save_to_buffer(timestamp, lat, lon, alt, speed):
    with open(BUFFER_FILE, "a", newline='') as f:
        writer = csv.writer(f)
        writer.writerow([timestamp, lat, lon, alt, speed])
    print("💾 Gespeichert im Puffer (Offline-Modus)")

def flush_buffer_to_db(cursor, db):
    if not os.path.exists(BUFFER_FILE):
        return
    with open(BUFFER_FILE,"r") as f:
        rows = list(csv.reader(f))
    if not rows:
        return

    success = True
    for row in rows:
        try:
            cursor.execute(
                """
                INSERT INTO gnss_data
                  (timestamp, latitude, longitude, altitude, speed)
                VALUES (%s,%s,%s,%s,%s)
                """,
                (row[0], float(row[1]), float(row[2]), float(row[3]), float(row[4]))
            )
        except Exception as e:
            print(f"❌ Fehler beim Nachtragen: {e}")
            success = False
            break

    if success:
        db.commit()
        open(BUFFER_FILE,"w").close()
        print(f"✅ {len(rows)} gepufferte Datensätze nachgetragen.")

# --------------------------------
# Hauptprogramm
# --------------------------------
last_speed = None

try:
    ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=1)
    print("✅ GNSS-Sensor verbunden!")

    print(f"→ verbinde zu {DB_NAME}@{DB_HOST}:{DB_PORT} als {DB_USER}")
    db     = psycopg2.connect(host=DB_HOST,port=DB_PORT,dbname=DB_NAME,
                              user=DB_USER,password=DB_PASSWORD,sslmode="require")
    cursor = db.cursor()
    print("✅ Mit Datenbank verbunden!")

    # Bereits gepufferte Daten nachtragen
    flush_buffer_to_db(cursor, db)

    while True:
        try:
            line = ser.readline().decode('utf-8', errors='ignore').strip()
            print(f"Empfangen: {line}")

            # 1) Speed aus GPRMC ziehen
            if line.startswith("$GPRMC"):
                speed = parse_gprmc(line)
                if speed is not None:
                    last_speed = speed
                    print(f"🚀 Speed aktualisiert: {last_speed:.2f} km/h")
                continue

            # 2) Positions-Daten aus GGA
            if "$GGA" in line:
                print("➡️ GGA-Zeile erkannt!")
                data = parse_gpgga(line)
                if data:
                    lat, lon, alt = data
                    timestamp = datetime.utcnow()
                    speed = last_speed  # zuletzt gelesener Speed (kann None sein)

                    print(f"🌍 Parsed: {lat}, {lon}, {alt} m  🚀 {speed} km/h")

                    try:
                        cursor.execute(
                            """
                            INSERT INTO gnss_data
                              (timestamp, latitude, longitude, altitude, speed)
                            VALUES (%s,%s,%s,%s,%s)
                            """,
                            (timestamp, lat, lon, alt, speed)
                        )
                        db.commit()
                        print(f"✅ Gespeichert in DB: {timestamp}")

                        # Puffer nach erfolgreichem Live-Insert leeren
                        flush_buffer_to_db(cursor, db)

                    except Exception as db_err:
                        print(f"❌ Fehler beim Insert: {db_err}")
                        save_to_buffer(timestamp,
                                       lat, lon, alt, speed)
                else:
                    print("⚠️ Parsing fehlgeschlagen – keine Daten!")

        except KeyboardInterrupt:
            print("\n🛑 GNSS-Logger beendet durch Tastatur")
            break

        except Exception as e:
            print(f"⚠️ Unerwarteter Fehler: {e}")
            time.sleep(2)

finally:
    print("\n📦 Aufräumen...")
    try:
        ser.close()
        cursor.close()
        db.close()
    except Exception as e:
        print(f"⚠️ Fehler beim Aufräumen: {e}")