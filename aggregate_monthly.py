from modules.toDF import notion_results_to_df
from modules.monthlyAggregate import get_or_create_monthly_summary_db, upsert_monthly_summary
from notion_client import Client
import pandas as pd
from dateutil import parser
from dotenv import load_dotenv
import os




# === LOAD ENV VARIABLES ===
load_dotenv()
NOTION_TOKEN = os.getenv("NOTION_TOKEN")

# === CONFIGURATION ===
SOURCE_DB_ID = "27fb645e06f880a6aa40c479b93cb235"      # source DB (shows dauly performance)
TARGET_DB_ID = "27fb645e06f8804e9dd3e08013f57bc1" # aggregated DB (see monthly performance)

# === CONNECT TO NOTION ===
notion = Client(auth=NOTION_TOKEN)

# === FETCH ALL RECORDS ===
results = []
response = notion.databases.query(database_id=SOURCE_DB_ID)
results.extend(response["results"])
while response.get("has_more"):
    response = notion.databases.query(
        database_id=SOURCE_DB_ID,
        start_cursor=response["next_cursor"]
    )
    results.extend(response["results"])





# use imported function


df = notion_results_to_df(results)
print("\n=== Daily Habit Data ===")
print(df.to_string(index=False))


# --- Aggregate monthly performance ---
import calendar

# Step 1 — Aggregate
monthly_avg = (
    df.groupby(["Year", "Month"], as_index=False)["rnd"]
      .mean()
      .rename(columns={"rnd": "avg_rnd"})
)

# Step 2 — Add numeric month value for proper sorting
monthly_avg["Month_Num"] = monthly_avg["Month"].apply(lambda m: list(calendar.month_name).index(m))

# Step 3 — Sort by Year (desc), then Month (desc)
monthly_avg = (
    monthly_avg
    .sort_values(["Year", "Month_Num"], ascending=[False, False])
    .drop(columns="Month_Num")
    .reset_index(drop=True)
)

# Step 4 — Round and show
monthly_avg["avg_rnd"] = monthly_avg["avg_rnd"].round(2)

print("\n=== Monthly Average rnd (Sorted by Month DESC) ===")
print(monthly_avg.to_string(index=False))





# CREATE AGGREGATED DB

PARENT_PAGE_ID = "27fb645e06f880b7aae4cb1a6602473d"  # your H-track-container page

# 1️⃣ Get or create the database
db_id = get_or_create_monthly_summary_db(notion, PARENT_PAGE_ID)

# 2️⃣ Upsert data
upsert_monthly_summary(notion, db_id, monthly_avg)



