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
# Globale Variablen
# -------------------------
ser = None
db = None
cursor = None

# -------------------------
# .env laden & prüfen
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
def connect_db():
    """(Re)connect to the database and flush any buffered rows."""
    global db, cursor
    try:
        if db:
            db.close()
    except:
        pass
    db = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD, sslmode="require"
    )
    cursor = db.cursor()
    print("🔄 (Re)connected to DB")
    flush_buffer_to_db()

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
    return speed_kn * 1.852  # kn → km/h

def save_to_buffer(ts, lat, lon, alt, speed):
    """Atomically prepend a row to BUFFER_FILE."""
    fd, tmp = tempfile.mkstemp(dir=".", prefix=BUFFER_FILE, text=True)
    with os.fdopen(fd, "w", newline="") as f_tmp:
        writer = csv.writer(f_tmp)
        writer.writerow([ts.isoformat(), lat, lon, alt, speed])
        if os.path.exists(BUFFER_FILE):
            with open(BUFFER_FILE, "r", newline="") as f_old:
                for row in f_old:
                    f_tmp.write(row)
    os.replace(tmp, BUFFER_FILE)
    print("💾 Gespeichert im Puffer (Offline-Modus)")

def flush_buffer_to_db():
    """Try to insert all buffered rows; keep only failures."""
    if not os.path.exists(BUFFER_FILE):
        return
    rows = list(csv.reader(open(BUFFER_FILE, "r", newline="")))
    if not rows:
        return

    success = 0
    failed = []

    for row in rows:
        try:
            cursor.execute(
                "INSERT INTO gnss_data (timestamp, latitude, longitude, altitude, speed) "
                "VALUES (%s,%s,%s,%s,%s)",
                (row[0], float(row[1]), float(row[2]), float(row[3]), float(row[4]))
            )
        except Exception as e:
            print(f"❌ Nachtrag-Fehler bei {row}: {e}")
            failed.append(row)
        else:
            success += 1

    if success > 0:
        try:
            db.commit()
        except Exception as e:
            print(f"⚠️ Commit-Fehler: {e} – Reconnecting for next attempt")
            connect_db()
            return

        with open(BUFFER_FILE, "w", newline="") as f:
            csv.writer(f).writerows(failed)
        print(f"✅ {success} gepufferte Datensätze nachgetragen; {len(failed)} verbleiben.")

# -------------------------
# Hauptprogramm
# -------------------------
if __name__ == "__main__":
    last_speed = None
    last_flush = time.time()

    try:
        ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=1)
        print("✅ GNSS-Sensor verbunden!")
        connect_db()

        while True:
            line = ser.readline().decode("utf-8", errors="ignore").strip()
            if not line:
                continue

            print(f"Empfangen: {line}")
            header = line.split(",")[0]
            print(f"[DEBUG] Header = {header}")

            # Speed aus RMC
            if header in ("$GPRMC", "$GNRMC"):
                speed = parse_gprmc(line)
                if speed is not None:
                    last_speed = speed
                    print(f"🚀 Speed aktualisiert: {last_speed:.2f} km/h")
                continue

            # Position aus GGA
            if header.endswith("GGA"):
                print("➡️ GGA-Zeile erkannt!")
                data = parse_gpgga(line)
                if not data:
                    print("⚠️ Parsing fehlgeschlagen")
                    continue
                lat, lon, alt = data
                ts = datetime.utcnow()
                speed = last_speed if last_speed is not None else 0.0
                print(f"🌍 Parsed: {lat}, {lon}, {alt} m  🚀 {speed:.2f} km/h")

                # Live-Insert mit reconnect-Check
                try:
                    if cursor is None or cursor.closed or db.closed:
                        connect_db()
                    cursor.execute(
                        "INSERT INTO gnss_data (timestamp, latitude, longitude, altitude, speed) "
                        "VALUES (%s,%s,%s,%s,%s)",
                        (ts.isoformat(), lat, lon, alt, speed)
                    )
                    db.commit()
                    print(f"✅ Gespeichert in DB: {ts.isoformat()}")
                    flush_buffer_to_db()
                except Exception as e:
                    print(f"❌ Insert-Fehler: {e}")
                    save_to_buffer(ts, lat, lon, alt, speed)
                    connect_db()

            # Periodischer reconnect + flush alle 30 Sekunden
            if time.time() - last_flush >= 30:
                try:
                    if cursor is None or cursor.closed or db.closed:
                        connect_db()
                    else:
                        cursor.execute("SELECT 1")
                    flush_buffer_to_db()
                except Exception as e:
                    print(f"⚠️ Periodischer Flush/Reconnect-Fehler: {e}")
                    connect_db()
                last_flush = time.time()

    except KeyboardInterrupt:
        print("\n🛑 GNSS-Logger beendet durch Tastatur")

    except Exception as e:
        print(f"⚠️ Unerwarteter Fehler: {e}")

    finally:
        print("\n📦 Aufräumen…")
        if ser:
            ser.close()
        if cursor:
            cursor.close()
        if db:
            db.close()