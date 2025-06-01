import json
import io
import time
from collections import defaultdict
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# === CONFIG ===
SERVICE_ACCOUNT_FILE = "service_account.json"
INPUT_FOLDER_ID = "1iJVmIgBLt2_WVA5AfOQzPalceUif2-Sd"     # reduced_votes
OUTPUT_FOLDER_ID = "1TkYp_wxZ9p1_vQ1Pj2-mrlNVMYlOOUoG"    # final rankings
LOGS_FOLDER_ID   = "1aTNKqDVg8wxrTScrhyoKcHYcY_cQEJqU"     # logs
CHECK_INTERVAL   = 30
TOTAL_RUNTIME    = 180

log_filename = f"ranking_log_shuffle2_{datetime.now():%Y%m%d_%H%M%S}.txt"

def log(message):
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    print(f"{timestamp} {message}", flush=True)
    with open(log_filename, "a") as f:
        f.write(f"{timestamp} {message}\n")

def authenticate():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds)

def upload_file(drive, filename, folder_id):
    file_metadata = {"name": filename, "parents": [folder_id]}
    media = MediaFileUpload(filename, mimetype="text/plain")
    return drive.files().create(body=file_metadata, media_body=media, fields="id").execute()

def delete_file_if_exists(drive, filename, folder_id):
    query = f"name='{filename}' and '{folder_id}' in parents"
    files = drive.files().list(q=query, fields="files(id)").execute().get("files", [])
    for f in files:
        drive.files().delete(fileId=f["id"]).execute()

def download_json_file(drive, file_id):
    request = drive.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return json.load(fh)

def process_vote_file(drive, file, total_votes):
    filename = file['name']
    file_id = file['id']
    country = filename.replace("reduced_votes_", "").replace(".json", "").lower()
    output_file = f"final_ranking_{country}.txt"

    log(f"⬇️ Downloaden van '{filename}'...")
    data = download_json_file(drive, file_id)

    country_votes = defaultdict(int)
    for entry in data:
        for vote in entry["votes"]:
            song = vote["song_number"]
            count = vote["count"]
            country_votes[song] += count
            total_votes[song] += count

    ranking = sorted(country_votes.items(), key=lambda x: x[1], reverse=True)
    with open(output_file, "w") as f:
        f.write(f"Final Song Ranking for {country.upper()}:\n\n")
        for i, (song, votes) in enumerate(ranking, start=1):
            f.write(f"{i}. Song {song}: {votes} votes\n")

    log(f"✅ '{output_file}' lokaal opgeslagen.")
    delete_file_if_exists(drive, output_file, OUTPUT_FOLDER_ID)
    upload_file(drive, output_file, OUTPUT_FOLDER_ID)
    log(f"📤 '{output_file}' geüpload naar Google Drive.")

def write_global_ranking(drive, total_votes):
    output = "global_winner_ranking.txt"
    ranking = sorted(total_votes.items(), key=lambda x: x[1], reverse=True)

    with open(output, "w") as f:
        f.write("Global Final Song Ranking:\n\n")
        for i, (song, votes) in enumerate(ranking, start=1):
            f.write(f"{i}. Song {song}: {votes} votes\n")

    delete_file_if_exists(drive, output, OUTPUT_FOLDER_ID)
    upload_file(drive, output, OUTPUT_FOLDER_ID)
    log(f"🌍 Globale ranking geüpload naar Google Drive als '{output}'.")

def main():
    drive = authenticate()
    total_votes = defaultdict(int)
    seen_files = set()
    start = time.time()

    log("🟢 Shuffle_2 gestart: stemverwerking voor 3 minuten...")

    try:
        while time.time() - start < TOTAL_RUNTIME:
            query = (
                f"'{INPUT_FOLDER_ID}' in parents "
                f"and name contains 'reduced_votes_' "
                f"and name contains '.json'"
            )
            result = drive.files().list(q=query, fields="files(id, name)").execute()
            for file in result.get("files", []):
                if file["name"] not in seen_files:
                    seen_files.add(file["name"])
                    process_vote_file(drive, file, total_votes)
            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        log("🛑 Handmatig gestopt met Ctrl+C.")

    log("📊 Genereer globale rangschikking...")
    write_global_ranking(drive, total_votes)

    try:
        upload_file(drive, log_filename, LOGS_FOLDER_ID)
        log("📁 Logbestand geüpload.")
    except Exception as e:
        log(f"⚠️ Fout bij log-upload: {e}")

    log("🏁 Verwerking afgerond.")

if __name__ == "__main__":
    main()
