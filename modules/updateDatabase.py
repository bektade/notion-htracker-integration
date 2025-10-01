from notion_client import Client

def get_or_create_monthly_summary_db(notion, parent_page_id):
    """
    Check if a database named 'Monthly Aggregate Summary' already exists inside the parent page.
    If it exists, return its ID.
    If not, create a new one with proper properties.
    """

    # Step 1️⃣ — List all blocks (children) inside the parent page
    blocks = notion.blocks.children.list(parent_page_id)["results"]

    # Step 2️⃣ — Check for existing inline or child database
    for block in blocks:
        if block["type"] == "child_database":
            db_title = block["child_database"]["title"].strip().lower()
            if db_title == "monthly aggregate summary".lower():
                print(f"✅ Found existing inline database: {db_title} ({block['id']})")
                return block["id"]

    # Step 3️⃣ — If not found, create a new database
    db = notion.databases.create(
        parent={"type": "page_id", "page_id": parent_page_id},
        title=[{"type": "text", "text": {"content": "Monthly Aggregate Summary"}}],
        icon={"type": "emoji", "emoji": "🧮"},  # aggregate icon
        properties={
            "Name": {"title": {}},  # ✅ Replaces "Month"
            "Average rnd": {"number": {"format": "number"}},  # ✅ your metric
            "Date": {"date": {}},  # ✅ month-level date
        },
    )

    print(f"🆕 Created new Notion database with correct properties: {db['id']}")
    return db["id"]




from datetime import datetime

def upsert_monthly_summary(notion, db_id, monthly_df):
    """
    Updates or inserts monthly summary records into an existing Notion database.
    """

    # Step 1️⃣ — Get all existing pages in the database
    existing_pages = []
    response = notion.databases.query(database_id=db_id)
    existing_pages.extend(response["results"])

    while response.get("has_more"):
        response = notion.databases.query(
            database_id=db_id, start_cursor=response["next_cursor"]
        )
        existing_pages.extend(response["results"])

    # Step 2️⃣ — Build lookup of existing entries (by Name)
    existing_map = {}
    for page in existing_pages:
        title_prop = page["properties"]["Name"]["title"]
        if title_prop:
            name_label = title_prop[0]["plain_text"]
            existing_map[name_label] = page["id"]

    # Step 3️⃣ — Update or insert
    for _, row in monthly_df.iterrows():
        month_label = f"{row['Month']} {int(row['Year'])}"
        avg_value = float(row["avg_rnd"])

        # Represent the month as a Notion date (first of the month)
        month_number = datetime.strptime(row["Month"], "%B").month
        date_value = f"{row['Year']}-{month_number:02d}-01"

        if month_label in existing_map:
            # ✅ Update existing
            notion.pages.update(
                page_id=existing_map[month_label],
                properties={
                    "Average rnd": {"number": avg_value},
                    "Date": {"date": {"start": date_value}},
                },
            )
            print(f"🔄 Updated: {month_label} → {avg_value}")
        else:
            # ➕ Insert new entry
            notion.pages.create(
                parent={"database_id": db_id},
                properties={
                    "Name": {"title": [{"text": {"content": month_label}}]},
                    "Average rnd": {"number": avg_value},
                    "Date": {"date": {"start": date_value}},
                },
            )
            print(f"➕ Added: {month_label} → {avg_value}")

    print("\n✅ Monthly summary successfully updated in Notion!")
