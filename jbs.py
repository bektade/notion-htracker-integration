from modules.notion_client import Notion

# === CONFIGURATION ===
# source DB (shows daily performance)
SOURCE_DB_ID = "1cbb645e06f880569df7d5f90c7842b3"   # Summer20252-III db
PARENT_PAGE_ID = "19e1fa7d3996492588ae15a332c343f2"    # Job_APP_Tracker_2025 page

# === CREATE NOTION CLIENT ===
jobs_apps = Notion(aggregation_attribute=None)



daily_df = jobs_apps.get_daily_data(database_id=SOURCE_DB_ID)

# === DISPLAY RESULTS ===
print("\n=== jb app Data ===")
print(daily_df.to_string(index=False))


