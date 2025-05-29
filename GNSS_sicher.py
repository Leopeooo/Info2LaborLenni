#!/usr/bin/env python3
import os
import time
import serial
import csv
import tempfile
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
    # Atomar in eine Temp-Datei schreiben und ersetzen
    tmp_fd, tmp_path = tempfile.mkstemp(dir=".", prefix=BUFFER_FILE, text=True)
    with os.fdopen(tmp_fd, "a", newline="") as f_tmp:
        writer = csv.writer(f_tmp)
        writer.writerow([timestamp.isoformat(), lat, lon, alt, speed])
        # falls es altes buffer.csv gibt, kopiere dessen Zeilen mit
        if os.path.exists(BUFFER_FILE):
            with open(BUFFER_FILE, "r", newline="") as f_old:
                for row in f_old:
                    f_tmp.write(row)
    os.replace(tmp_path, BUFFER_FILE)
    print("💾 Gespeichert im Puffer (Offline-Modus)")

def flush_buffer_to_db(cursor, db):
    if not os.path.exists(BUFFER_FILE):
        return
    rows = list(csv.reader(open(BUFFER_FILE, "r", newline="")))
    if not rows:
        return

    success_count = 0
    remaining = []

    for row in rows:
        try:
            cursor.execute(
                """
                INSERT INTO gnss_data
                  (timestamp, latitude, longitude, altitude, speed)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    row[0],            # ISO-Timestamp string
                    float(row[1]),
                    float(row[2]),
                    float(row[3]),
                    float(row[4]),
                )
            )
        except Exception as e:
            print(f"❌ Fehler beim Nachtragen der Zeile {row}: {e} – überspringe")
            remaining.append(row)
        else:
            success_count += 1

    if success_count > 0:
        db.commit()
        # Schreibe nur nicht erfolgreiche Zeilen zurück in den Puffer
        with open(BUFFER_FILE, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(remaining)
        print(f"✅ {success_count} gepufferte Datensätze nachgetragen; {len(remaining)} verbleiben.")

# -------------------------
# Hauptprogramm
# -------------------------
if __name__ == "__main__":
    last_speed = None
    last_flush = time.time()

    try:
        # GNSS-Sensor öffnen
        ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=1)
        print("✅ GNSS-Sensor verbunden!")

        # Datenbank-Verbindung
        print(f"→ verbinde zu {DB_NAME}@{DB_HOST}:{DB_PORT} als {DB_USER}")
        db = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
            sslmode="require"
        )
        cursor = db.cursor()
        print("✅ Mit Datenbank verbunden!")

        # Puffer einmal beim Start nachtragen
        flush_buffer_to_db(cursor, db)

        # Endlosschleife
        while True:
            raw = ser.readline()
            line = raw.decode("utf-8", errors="ignore").strip()
            if not line:
                continue

            print(f"Empfangen: {line}")
            header = line.split(",")[0]
            print(f"[DEBUG] Header = {header}")

            # Geschwindigkeit aus RMC
            if header in ("$GPRMC", "$GNRMC"):
                speed = parse_gprmc(line)
                if speed is not None:
                    last_speed = speed
                    print(f"🚀 Speed aktualisiert: {last_speed:.2f} km/h")
                continue

            # Positionsdaten aus GGA
            if header.endswith("GGA"):
                print("➡️ GGA-Zeile erkannt!")
                data = parse_gpgga(line)
                if not data:
                    print("⚠️ Parsing fehlgeschlagen – keine Daten!")
                    continue
                lat, lon, alt = data
                timestamp = datetime.utcnow()
                speed = last_speed if last_speed is not None else 0.0

                print(f"🌍 Parsed: {lat}, {lon}, {alt} m  🚀 {speed:.2f} km/h")

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
                    print(f"✅ Gespeichert in DB: {timestamp.isoformat()}")
                    flush_buffer_to_db(cursor, db)

                except Exception as db_err:
                    print(f"❌ Insert-Fehler: {db_err}")
                    save_to_buffer(timestamp, lat, lon, alt, speed)

            # Periodischer Flush alle 30 Sekunden
            if time.time() - last_flush >= 30:
                flush_buffer_to_db(cursor, db)
                last_flush = time.time()

    except KeyboardInterrupt:
        print("\n🛑 GNSS-Logger beendet durch Tastatur")

    except Exception as e:
        print(f"⚠️ Unerwarteter Fehler im Hauptprogramm: {e}")

    finally:
        print("\n📦 Aufräumen…")
        if ser is not None:
            ser.close()
        if cursor is not None:
            cursor.close()
        if db is not None:
            db.close()