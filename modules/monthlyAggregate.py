from notion_client import Client

def get_or_create_monthly_summary_db(notion, parent_page_id):
    """
    Check if a database named 'Monthly Aggregate Summary' already exists inside the parent page.
    If it exists, return its ID.
    If not, create a new one and return its ID.
    """

    # Step 1️⃣ — List all blocks (children) inside the parent page
    blocks = notion.blocks.children.list(parent_page_id)["results"]

    # Step 2️⃣ — Check if any block is a database (inline or full-page)
    for block in blocks:
        # some blocks of type 'child_database' = inline DB
        if block["type"] == "child_database":
            db_title = block["child_database"]["title"].strip().lower()
            if db_title == "monthly aggregate summary".lower():
                print(f"✅ Found existing inline database: {db_title} ({block['id']})")
                return block["id"]

    # Step 3️⃣ — If no database found, create a new one
    db = notion.databases.create(
        parent={"type": "page_id", "page_id": parent_page_id},
        title=[{"type": "text", "text": {"content": "Monthly Aggregate Summary"}}],
        icon={"type": "emoji", "emoji": "🧮"},  # or 📊 / ✅
        properties={
            "Month": {"title": {}},
            "Average rnd": {"number": {"format": "number"}}
        }
    )

    print(f"🆕 Created new Notion database: {db['id']}")
    return db["id"]




def upsert_monthly_summary(notion, db_id, monthly_df):
    """
    Updates or inserts monthly summary records into an existing Notion database.
    """

    # Step 1: Get all existing pages in the database
    existing_pages = []
    response = notion.databases.query(database_id=db_id)
    existing_pages.extend(response["results"])

    while response.get("has_more"):
        response = notion.databases.query(
            database_id=db_id, start_cursor=response["next_cursor"]
        )
        existing_pages.extend(response["results"])

    # Step 2: Build a lookup of existing months
    existing_map = {}
    for page in existing_pages:
        title_prop = page["properties"]["Month"]["title"]
        if title_prop:
            month_label = title_prop[0]["plain_text"]
            existing_map[month_label] = page["id"]

    # Step 3: Update or insert
    for _, row in monthly_df.iterrows():
        month_label = f"{row['Month']} {int(row['Year'])}"
        avg_value = float(row["avg_rnd"])

        if month_label in existing_map:
            # ✅ Update existing entry
            notion.pages.update(
                page_id=existing_map[month_label],
                properties={"Average rnd": {"number": avg_value}},
            )
            print(f"🔄 Updated: {month_label} → {avg_value}")
        else:
            # ➕ Insert new entry
            notion.pages.create(
                parent={"database_id": db_id},
                properties={
                    "Month": {"title": [{"text": {"content": month_label}}]},
                    "Average rnd": {"number": avg_value},
                },
            )
            print(f"➕ Added: {month_label} → {avg_value}")

    print("\n✅ Monthly summary successfully updated in Notion!")
