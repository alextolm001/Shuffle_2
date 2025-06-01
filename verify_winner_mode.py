import json
import io
import time
from collections import defaultdict
from statistics import mode, StatisticsError
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# === CONFIG ===
SERVICE_ACCOUNT_FILE = "service_account.json"
INPUT_FOLDER_ID = "1iJVmIgBLt2_WVA5AfOQzPalceUif2-Sd"
OUTPUT_FOLDER_ID = "1TkYp_wxZ9p1_vQ1Pj2-mrlNVMYlOOUoG"
LOGS_FOLDER_ID = "1aTNKqDVg8wxrTScrhyoKcHYcY_cQEJqU"
RUNTIME_SECONDS = 180
CHECK_INTERVAL = 30
log_filename = f"ranking_log_mode_{datetime.now():%Y%m%d_%H%M%S}.txt"

def log(message):
    timestamp = f"[{datetime.now():%Y-%m-%d %H:%M:%S}]"
    print(f"{timestamp} {message}", flush=True)
    with open(log_filename, "a") as f:
        f.write(f"{timestamp} {message}\n")

def authenticate():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds)

def download_json_from_drive(service, file_id):
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return json.load(fh)

def safe_mode(values):
    try:
        return mode(values)
    except StatisticsError:
        return None

def upload_or_replace_on_drive(service, filename, folder_id):
    # verwijder oude versies
    existing = service.files().list(
        q=f"name='{filename}' and '{folder_id}' in parents",
        fields="files(id)"
    ).execute().get("files", [])
    for f in existing:
        service.files().delete(fileId=f["id"]).execute()

    media = MediaFileUpload(filename, mimetype="text/plain")
    service.files().create(
        body={"name": filename, "parents": [folder_id]},
        media_body=media,
        fields="id"
    ).execute()

def process_file(service, file, total_votes, total_flat_votes):
    name = file["name"]
    file_id = file["id"]
    country = name.replace("reduced_votes_", "").replace(".json", "").lower()
    output_file = f"final_ranking_mode_{country}.txt"

    log(f"📄 Verwerken van: {name}")
    data = download_json_from_drive(service, file_id)

    country_votes = defaultdict(int)
    flat_votes = []

    for record in data:
        for vote in record["votes"]:
            song = vote["song_number"]
            count = vote["count"]
            country_votes[song] += count
            total_votes[song] += count
            flat_votes.extend([song] * count)
            total_flat_votes.extend([song] * count)

    ranked = sorted(country_votes.items(), key=lambda x: x[1], reverse=True)
    most_common = safe_mode(flat_votes)
    top_song, top_votes = ranked[0]

    lines = [
        f"🎵 Most voted song using mode(): Song {most_common}",
        f"🏆 Top-ranked song by total votes: Song {top_song} with {top_votes} votes",
        "✅ Both methods agree on the winner!" if most_common == top_song else
        "⚠️ Mode and total vote count give different winners!"
    ]

    with open(output_file, "w") as f:
        f.write("\n".join(lines) + "\n")

    log(f"💾 Resultaten opgeslagen in {output_file}")
    upload_or_replace_on_drive(service, output_file, OUTPUT_FOLDER_ID)
    log(f"☁️ Upload voltooid: {output_file}")

def generate_global_results(service, total_votes, total_flat_votes):
    ranked = sorted(total_votes.items(), key=lambda x: x[1], reverse=True)
    most_common = safe_mode(total_flat_votes)
    top_song, top_votes = ranked[0]

    filename = "global_winner_ranking_mode.txt"
    lines = [
        f"🌍 TOTAAL: Most voted song using mode(): Song {most_common}",
        f"🌍 TOTAAL: Top-ranked song by total votes: Song {top_song} with {top_votes} votes",
        "✅ Both methods agree on the global winner!" if most_common == top_song else
        "⚠️ Disagreement between mode and total votes in global ranking."
    ]

    with open(filename, "w") as f:
        f.write("\n".join(lines) + "\n")
    for line in lines:
        log(line)

    upload_or_replace_on_drive(service, filename, OUTPUT_FOLDER_ID)
    log(f"📤 Globale ranking geüpload: {filename}")

def main():
    drive = authenticate()
    seen = set()
    total_votes = defaultdict(int)
    all_flat_votes = []
    start = time.time()

    log("🔁 Start verificatie met mode-methode...")

    try:
        while time.time() - start < RUNTIME_SECONDS:
            query = (
                f"'{INPUT_FOLDER_ID}' in parents and "
                f"name contains 'reduced_votes_' and name contains '.json'"
            )
            files = drive.files().list(q=query, fields="files(id, name)").execute().get("files", [])
            for file in files:
                if file["name"] not in seen:
                    seen.add(file["name"])
                    process_file(drive, file, total_votes, all_flat_votes)
            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        log("🛑 Script handmatig onderbroken.")

    if total_votes:
        log("📊 Genereren van globale resultaten...")
        generate_global_results(drive, total_votes, all_flat_votes)

    upload_or_replace_on_drive(drive, log_filename, LOGS_FOLDER_ID)
    log("✅ Logbestand geüpload.")
    log("🏁 Script afgesloten.")

if __name__ == "__main__":
    main()

