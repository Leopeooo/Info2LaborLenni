#!/usr/bin/env python3
import os
import time
import serial
import csv
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
import psycopg2

# -------------------------
# Variablen vorab anlegen
# -------------------------
ser = None
db = None
cursor = None

# -------------------------
# .env Datei laden & prüfen
# -------------------------
env_path = Path.cwd() / ".env"
if not env_path.exists():
    raise FileNotFoundError(f".env file not found at {env_path}")
load_dotenv(dotenv_path=env_path)

SERIAL_PORT = os.getenv("SERIAL_PORT", "/dev/serial0")
BAUD_RATE   = int(os.getenv("BAUD_RATE", "9600"))
BUFFER_FILE = "buffer.csv"

DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Pflicht-Variablen prüfen
for var in ("DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"):
    if not globals()[var]:
        raise RuntimeError(f"Environment variable {var} is not set in .env")

# -------------------------
# Hilfsfunktionen
# -------------------------
def convert_to_decimal(raw: str, direction: str):
    if not raw or not direction:
        return None
    deg  = int(float(raw) / 100)
    minu = float(raw) - deg * 100
    dec  = deg + minu / 60.0
    if direction in ("S", "W"):
        dec = -dec
    return dec

def parse_gpgga(line: str):
    parts = line.split(",")
    if len(parts) < 10:
        return None
    lat = convert_to_decimal(parts[2], parts[3])
    lon = convert_to_decimal(parts[4], parts[5])
    try:
        alt = float(parts[9])
    except ValueError:
        alt = None
    return lat, lon, alt

def parse_gprmc(line: str):
    parts = line.split(",")
    if len(parts) < 8 or not parts[7]:
        return None
    speed_kn = float(parts[7])
    return speed_kn * 1.852  # Knoten → km/h

def save_to_buffer(timestamp, lat, lon, alt, speed):
    with open(BUFFER_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([timestamp, lat, lon, alt, speed])
    print("💾 Gespeichert im Puffer (Offline-Modus)")

def flush_buffer_to_db(cursor, db):
    if not os.path.exists(BUFFER_FILE):
        return
    with open(BUFFER_FILE, "r") as f:
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
                VALUES (%s, %s, %s, %s, %s)
                """,
                (row[0], float(row[1]), float(row[2]), float(row[3]), float(row[4]))
            )
        except Exception as e:
            print(f"❌ Fehler beim Nachtragen: {e}")
            success = False
            break
    if success:
        db.commit()
        open(BUFFER_FILE, "w").close()
        print(f"✅ {len(rows)} gepufferte Datensätze nachgetragen.")

# -------------------------
# Hauptprogramm
# -------------------------
if __name__ == "__main__":
    last_speed = None

    try:
        # GNSS-Sensor öffnen
        ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=1)
        print("✅ GNSS-Sensor verbunden!")

        # Datenbank-Verbindung aufbauen
        print(f"→ verbinde zu {DB_NAME}@{DB_HOST}:{DB_PORT} als {DB_USER}")
        db = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            sslmode="require"
        )
        cursor = db.cursor()
        print("✅ Mit Datenbank verbunden!")

        # Puffer nachtragen falls vorhanden
        flush_buffer_to_db(cursor, db)

        # Endlosschleife: NMEA-Daten lesen
        while True:
            try:
                raw = ser.readline()
                line = raw.decode("utf-8", errors="ignore").strip()
                if not line:
                    continue
                print(f"Empfangen: {line}")

                # Header (Talker+Sentence) extrahieren
                header = line.split(",")[0]

                # 1) Geschwindigkeit aus RMC
                if header.endswith("RMC"):
                    speed = parse_gprmc(line)
                    if speed is not None:
                        last_speed = speed
                        print(f"🚀 Speed aktualisiert: {last_speed:.2f} km/h")
                    continue

                # 2) Positionsdaten aus GGA
                if header.endswith("GGA"):
                    print("➡️ GGA-Zeile erkannt!")
                    data = parse_gpgga(line)
                    if data:
                        lat, lon, alt = data
                        timestamp = datetime.utcnow()
                        speed = last_speed

                        print(f"🌍 Parsed: {lat}, {lon}, {alt} m  🚀 {speed} km/h")
                        try:
                            cursor.execute(
                                """
                                INSERT INTO gnss_data
                                  (timestamp, latitude, longitude, altitude, speed)
                                VALUES (%s, %s, %s, %s, %s)
                                """,
                                (timestamp, lat, lon, alt, speed)
                            )
                            db.commit()
                            print(f"✅ Gespeichert in DB: {timestamp}")
                            flush_buffer_to_db(cursor, db)

                        except Exception as db_err:
                            print(f"❌ Fehler beim Insert: {db_err}")
                            save_to_buffer(timestamp, lat, lon, alt, speed)
                    else:
                        print("⚠️ Parsing fehlgeschlagen – keine Daten!")

            except KeyboardInterrupt:
                print("\n🛑 GNSS-Logger beendet durch Tastatur")
                break

            except Exception as e:
                print(f"⚠️ Unerwarteter Fehler: {e}")
                time.sleep(2)

    finally:
        print("\n📦 Aufräumen…")
        if ser is not None:
            ser.close()
        if cursor is not None:
            cursor.close()
        if db is not None:
            db.close()