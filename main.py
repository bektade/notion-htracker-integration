from modules.notion_client import Notion

# === CONFIGURATION ===
# source DB (shows daily performance)
SOURCE_DB_ID = "27fb645e06f880a6aa40c479b93cb235"
PARENT_PAGE_ID = "27fb645e06f880b7aae4cb1a6602473d"    # your H-track-container page

# === CREATE NOTION CLIENT ===
notion = Notion(aggregation_attribute="rnd")  # attribute to aggregate on

# === PROCESS HABIT TRACKING DATA ===
print("🚀 Starting habit tracking data processing...")

# Complete workflow: fetch, aggregate, and update on contaner notion page
daily_df, monthly_df, summary_db_id = notion.process_habit_tracking(
    source_db_id=SOURCE_DB_ID,
    parent_page_id=PARENT_PAGE_ID
)

# === DISPLAY RESULTS ===
print("\n=== Daily Habit Data ===")
print(daily_df.to_string(index=False))

print("\n=== Monthly Average rnd (Sorted by Month DESC) ===")
print(monthly_df.to_string(index=False))

print(f"\n✅ Processing complete! Summary database ID: {summary_db_id}")
