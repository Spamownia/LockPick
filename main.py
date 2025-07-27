import os
import ftplib
from datetime import datetime

# Konfiguracja FTP
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"

# Katalog lokalny do zapisu logów
LOCAL_DIR = "downloaded_logs"

def debug(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [DEBUG] {msg}")

def connect_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
    ftp.login(FTP_USER, FTP_PASS)
    debug(f"Połączono z FTP: {FTP_HOST}:{FTP_PORT}")
    return ftp

def list_log_files(ftp):
    try:
        ftp.cwd(FTP_DIR)
        debug(f"Zmieniono katalog: {FTP_DIR}")
        files = []
        ftp.retrlines('LIST', lambda line: files.append(line))

        # Filtrowanie tylko plików typu gameplay*.log
        gameplay_files = []
        for line in files:
            parts = line.split()
            filename = parts[-1]
            if filename.startswith("gameplay") and filename.endswith(".log"):
                gameplay_files.append(filename)

        debug(f"Znaleziono {len(gameplay_files)} plików gameplay_*.log")
        return gameplay_files
    except Exception as e:
        debug(f"Błąd przy pobieraniu listy plików: {e}")
        return []

def download_files(ftp, filenames):
    os.makedirs(LOCAL_DIR, exist_ok=True)
    downloaded = 0

    for filename in filenames:
        local_path = os.path.join(LOCAL_DIR, filename)
        try:
            with open(local_path, "wb") as f:
                ftp.retrbinary(f"RETR {filename}", f.write)
            debug(f"Pobrano: {filename}")
            downloaded += 1
        except Exception as e:
            debug(f"Błąd podczas pobierania {filename}: {e}")
    debug(f"Pobrano {downloaded} plików do: {LOCAL_DIR}")

def main():
    debug("Start programu")

    try:
        ftp = connect_ftp()
        files = list_log_files(ftp)
        download_files(ftp, files)
        ftp.quit()
        debug("Zakończono połączenie FTP")
    except Exception as e:
        debug(f"Błąd krytyczny: {e}")

if __name__ == "__main__":
    main()
