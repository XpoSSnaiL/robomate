import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from collections import Counter

# Airtable configuration
BASE_ID = "apphxXvfsb9LwRMNy"
TABLE = "Requests"
CONSULTANTS_TABLE = "Consultants"
API_TOKEN = "patiI7eldnarQBITy.5c2e304bd90f127c8789622852ecec1d10a728ad199f3f77155e85d5be00bb41"
HEADERS = {"Authorization": f"Bearer {API_TOKEN}"}

# Fetch data from Airtable
def fetch_requests():
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE}"
    all_records = []
    offset = None
    while True:
        params = {"offset": offset} if offset else {}
        response = requests.get(url, headers=HEADERS, params=params).json()
        all_records.extend(response.get("records", []))
        offset = response.get("offset")
        if not offset:
            break
    return all_records

def fetch_consultants():
    url = f"https://api.airtable.com/v0/{BASE_ID}/{CONSULTANTS_TABLE}"
    all_records = []
    offset = None

    while True:
        params = {"offset": offset} if offset else {}
        response = requests.get(url, headers=HEADERS, params=params).json()
        all_records.extend(response.get("records", []))
        offset = response.get("offset")
        if not offset:
            break

    # dict: {record_id: consultant_name}
    return {
        r["id"]: r["fields"].get("Name", "Unknown")
        for r in all_records
    }


# Process data
records = fetch_requests()
data = []
for r in records:
    fields = r.get("fields", {})
    data.append({
        "RequestId": fields.get("Request Id"),
        "Status": fields.get("Status"),
        "AssignedConsultant": fields.get("Assigned consultant", [None])[0] if fields.get("Assigned consultant") else None,
        "CreatedAt": fields.get("Created at"),
        "TakenIntoWorkAt": fields.get("Taken Into Work At"),
        "ClosedAt": fields.get("Closed at")
    })

consultant_map = fetch_consultants()
df = pd.DataFrame(data)

# Convert Airtable datetimes to timezone-aware UTC
df["CreatedAt"] = pd.to_datetime(df["CreatedAt"], utc=True)
df["ClosedAt"] = pd.to_datetime(df["ClosedAt"], utc=True)
df["TakenIntoWorkAt"] = pd.to_datetime(df["TakenIntoWorkAt"], utc=True)
df["ConsultantName"] = df["AssignedConsultant"].map(consultant_map)

# Weekly metrics
today = datetime.now(timezone.utc)  # timezone-aware current datetime
week_start = (today - timedelta(days=today.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
week_end = week_start + timedelta(days=7)

# Filter weekly data
weekly_df = df[(df["CreatedAt"] >= week_start) & (df["CreatedAt"] < week_end)]

# Compute metrics
num_new_requests = len(weekly_df)
num_closed_requests = len(weekly_df[weekly_df["Status"] == "Closed"])
weekly_df["ProcessingTime"] = (weekly_df["ClosedAt"] - weekly_df["TakenIntoWorkAt"]).dt.total_seconds() / 3600
avg_processing_time = weekly_df["ProcessingTime"].mean()

# Top consultants
consultant_counts = Counter(weekly_df[weekly_df["Status"] == "Closed"]["ConsultantName"])
top_3_consultants = consultant_counts.most_common(3)

# Save report
report = {
    "WeekStart": week_start.strftime("%Y-%m-%d"),
    "WeekEnd": week_end.strftime("%Y-%m-%d"),
    "NewRequests": num_new_requests,
    "ClosedRequests": num_closed_requests,
    "AvgProcessingTimeHours": avg_processing_time,
    "Top3Consultants": top_3_consultants
}

pd.DataFrame([report]).to_csv("weekly_report.csv", index=False)
print("Report saved as weekly_report.csv")
