import os
import time
import json
import numpy as np
import requests
import pandas as pd
import gspread
from datetime import datetime
from zoneinfo import ZoneInfo
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from concurrent.futures import ThreadPoolExecutor

# -------------------- START TIMER --------------------
start_time = time.time()

# -------------------- TIMEZONE --------------------
IST = ZoneInfo("Asia/Kolkata")

def to_ist(series):
    """Convert datetime series to IST.
    - tz-aware (e.g. +05:30 or UTC): convert directly to IST
    - tz-naive: data is already in IST, just localize
    """
    if series.dt.tz is not None:
        return series.dt.tz_convert(IST)
    else:
        return series.dt.tz_localize(IST)

def strip_tz(df):
    """Convert tz-aware datetime columns to plain IST strings for Sheets.
    This prevents gspread from reinterpreting timezone info during write.
    """
    for col in df.select_dtypes(include='datetimetz').columns:
        df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    return df

# -------------------- ENV & AUTH --------------------
METABASE_API_KEY = os.getenv("METABASE_API_KEY")
service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")

# -------------------- HARDCODED CONFIG --------------------
TA_SHEET_KEY = "1gx-0pc1OD4KayvwSv70rLOTOxPHnJNCONyQDDiaUdhU"

MB_BASE = "https://metabase-lierhfgoeiwhr.newtonschool.co"
TA_SESSIONS_QUERY = f"{MB_BASE}/api/card/6135/query/json"
TA_BATCH_QUERY    = f"{MB_BASE}/api/card/6570/query/json"
TA_SLOTS_QUERY    = f"{MB_BASE}/api/card/6213/query/json"

if not METABASE_API_KEY or not service_account_json:
    raise ValueError("❌ Missing environment variables. Check GitHub secrets.")

# -------------------- GOOGLE AUTH --------------------
service_info = json.loads(service_account_json)
creds = Credentials.from_service_account_info(
    service_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)
gc = gspread.authorize(creds)

# -------------------- METABASE AUTH --------------------
METABASE_HEADERS = {
    'Content-Type': 'application/json',
    'x-api-key': METABASE_API_KEY
}
print("✅ Using Metabase API key auth")
# -------------------- UTILITIES --------------------
def fetch_with_retry(url, headers, retries=5, delay=15):
    for attempt in range(1, retries + 1):
        try:
            r = requests.post(url, headers=headers, timeout=120)
            r.raise_for_status()
            return r
        except Exception as e:
            print(f"[Metabase] Attempt {attempt} failed: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                raise

def safe_clear_and_update(worksheet, df):
    """Clears sheet and writes fresh data with retry logic."""
    title = worksheet.title
    print(f"🔄 Updating sheet: {title}")
    for attempt in range(1, 6):
        try:
            worksheet.clear()
            set_with_dataframe(worksheet, df, include_index=False, include_column_header=True)
            print(f"✅ Successfully updated: {title}")
            return
        except Exception as e:
            print(f"[Sheets] Attempt {attempt} failed for {title}: {e}")
            if attempt < 5:
                time.sleep(20)
            else:
                print(f"❌ All attempts failed for {title}.")
                raise

# -------------------- FETCH ALL 3 QUERIES IN PARALLEL --------------------
print("📡 Fetching TA Sessions, Batch, and Slots data in parallel...")

urls = {
    "sessions": TA_SESSIONS_QUERY,
    "batch":    TA_BATCH_QUERY,
    "slots":    TA_SLOTS_QUERY
}

with ThreadPoolExecutor(max_workers=3) as executor:
    futures = {name: executor.submit(fetch_with_retry, url, METABASE_HEADERS) for name, url in urls.items()}
    results = {name: f.result() for name, f in futures.items()}

print("✅ All queries fetched successfully")
# -------------------- DEBUG: inspect raw responses --------------------
for name, resp in results.items():
    print(f"🔍 [{name}] status={resp.status_code} content-type={resp.headers.get('Content-Type')}")
    print(f"🔍 [{name}] body preview: {resp.text[:300]!r}")

# -------------------- PROCESS df1 (Sessions Feedback) --------------------
df1 = pd.DataFrame(results["sessions"].json())
# -------------------- PROCESS df1 (Sessions Feedback) --------------------
df1 = pd.DataFrame(results["sessions"].json())
df1 = df1[[
    'subjective_feedback', 'lu_batch_name', 'au_batch_name', 'au_start_date',
    'feedback_given', 'session_id', 'rating', 'description', 'module_name',
    'topic', 'cancel_reason', 'action_time', 'booked_time', 'start_timestamp', 'doubt_type', 'doubt_description',
    'question_link','student_rating','student_behaviour','doubt_part_cirricullum','student_intent','type_of_doubt',
    'additional_feedback', 'gem_status'
]]
df1 = df1.rename(columns={
    'lu_batch_name': 'Batch',
    'module_name': 'Module',
    'start_timestamp': 'session_start_time'
})

# Parse and convert all datetime columns to IST
df1['session_start_time'] = to_ist(pd.to_datetime(df1['session_start_time'], errors='coerce'))
df1['action_time']        = to_ist(pd.to_datetime(df1['action_time'],        errors='coerce'))
df1['booked_time']        = to_ist(pd.to_datetime(df1['booked_time'],        errors='coerce'))

# -------------------- PROCESS df2 (Batch Info) --------------------
df2 = pd.DataFrame(results["batch"].json())
df2 = df2.rename(columns={'batch': 'Batch'})

# Drop session_start_time from df2 if it exists to avoid _x/_y conflict after merge
df2 = df2.drop(columns=['session_start_time'], errors='ignore')

# Verify required merge columns exist in df2
required_df2_cols = {'session_id', 'Batch', 'mentor_name', 'time_category'}
missing = required_df2_cols - set(df2.columns)
if missing:
    raise ValueError(f"❌ df2 (batch query) is missing expected columns: {missing}")

# -------------------- MERGE df1 + df2 --------------------
# Join on session_id only. Card 6135 gives Batch = the student's module batch (e.g. "DS SQL - July A 2026"),
# while card 6570's batch is the course the session was BOOKED under — for PCDS students that is usually
# "Pre Course DS Certification - <month>". Joining on both dropped every session where the two differ
# (most PCDS sessions). Batch is kept from card 6135.
df2 = df2.drop(columns=['Batch']).drop_duplicates(subset=['session_id'])
df = pd.merge(df1, df2, on='session_id', how='inner')

# -------------------- FEATURE ENGINEERING --------------------
df['au_start_date'] = pd.to_datetime(df['au_start_date'], errors='coerce')

df['month_diff_period'] = (
    df['session_start_time'].dt.to_period('M').astype(int) -
    df['au_start_date'].dt.to_period('M').astype(int)
)

# Safer rating deduplication — uses positional index within group, not global DataFrame index
df['rating'] = df.groupby('session_id')['rating'].transform(
    lambda x: x.mask(np.arange(len(x)) != 0, np.nan)
)

# Derive year_month_date_hour from IST session_start_time
df['year_month_date_hour'] = df['session_start_time'].dt.strftime('%Y-%m-%d-%H')
df = df.drop_duplicates()

# -------------------- PROCESS df3 (TA Slots) --------------------
df3 = pd.DataFrame(results["slots"].json())

# Parse and convert date to IST
df3['date'] = to_ist(pd.to_datetime(df3['date']))

# Derive year_month_date_hour from IST date
df3['year_month_date_hour'] = df3['date'].dt.strftime('%Y-%m-%d-%H')
df3 = df3.rename(columns={'ta': 'mentor_name'})

# -------------------- MERGE df + df3 → df4 --------------------
df4 = pd.merge(df, df3, on=['year_month_date_hour', 'mentor_name', 'time_category'], how='outer')

# -------------------- GEM / PROGRAM COLUMNS (Oct 2026) --------------------
# `GEM Status` (GEM / Non-GEM / Not tagged, from card 6135) and `Program` (PCDS / Adv+ / ...)
# are added as the LAST two columns so existing Looker fields don't shift.
def program_from_batch(name):
    n = str(name or '').lower()
    if not n or n == 'nan':
        return 'Other'
    if 'advantage plus' in n or 'advantage +' in n or 'adv+' in n:
        return 'Adv+'
    if 'advantage' in n:
        return 'Advantage'
    if 'agentic' in n:
        return 'Agentic AI'
    if 'xcelerate' in n:
        return 'Xcelerate'
    if 'data science' in n or 'pcds' in n:
        return 'PCDS'
    return 'Other'


def add_segment_cols(frame):
    frame = frame.copy()
    if 'gem_status' in frame.columns:
        frame = frame.rename(columns={'gem_status': 'GEM Status'})
    if 'GEM Status' in frame.columns:
        frame['GEM Status'] = frame['GEM Status'].fillna('Not tagged')
    if 'au_batch_name' in frame.columns:
        frame['Program'] = frame['au_batch_name'].apply(program_from_batch)
    last = [c for c in ['GEM Status', 'Program'] if c in frame.columns]
    return frame[[c for c in frame.columns if c not in last] + last]


df  = add_segment_cols(df)
df4 = add_segment_cols(df4)

# -------------------- STRIP TIMEZONE BEFORE WRITING TO SHEETS --------------------
# Convert tz-aware datetime columns to plain IST strings
# This prevents gspread from reinterpreting timezone info during write
df  = strip_tz(df.copy())
df3 = strip_tz(df3.copy())
df4 = strip_tz(df4.copy())

# -------------------- WRITE TO GOOGLE SHEETS --------------------
print("📝 Connecting to Google Sheets...")
sheet = gc.open_by_key(TA_SHEET_KEY)

safe_clear_and_update(sheet.worksheet("TA_sessions"),     df)
time.sleep(3)
safe_clear_and_update(sheet.worksheet("TA_slots"),        df3)
time.sleep(3)
safe_clear_and_update(sheet.worksheet("TA-sessions-all"), df4)

# -------------------- TIMESTAMP --------------------
current_time = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%d-%b-%Y %H:%M:%S")
print(f"✅ Timestamp: {current_time}")

# -------------------- TIMER --------------------
end_time = time.time()
mins, secs = divmod(end_time - start_time, 60)
print(f"⏱ Total time: {int(mins)}m {int(secs)}s")
print("🎯 TA Sessions + Slots pipeline completed successfully!")
