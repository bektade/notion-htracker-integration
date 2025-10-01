"""
Notion OOP Client for Habit Tracking Integration

This module provides a class-based interface for interacting with Notion databases,
specifically designed for habit tracking and data aggregation workflows.
"""

import pandas as pd
from dateutil import parser
from notion_client import Client
from datetime import datetime
import calendar
from dotenv import load_dotenv
import os


class Notion:
    """
    A class to handle Notion database operations for habit tracking.
    
    This class provides methods to:
    - Connect to Notion API
    - Fetch data from source databases
    - Convert data to DataFrames
    - Aggregate monthly performance
    - Create and update summary databases
    """
    
    def __init__(self, notion_token=None):
        """
        Initialize the Notion client.
        
        Args:
            notion_token (str, optional): Notion API token. If not provided,
                                        will try to load from environment variables.
        """
        if notion_token is None:
            load_dotenv()
            notion_token = os.getenv("NOTION_TOKEN")
        
        if not notion_token:
            raise ValueError("Notion token is required. Provide it directly or set NOTION_TOKEN environment variable.")
        
        self.client = Client(auth=notion_token)
        self.source_db_id = None
        self.target_db_id = None
        self.parent_page_id = None
    
    def set_database_ids(self, source_db_id, target_db_id=None, parent_page_id=None):
        """
        Set the database IDs for source and target databases.
        
        Args:
            source_db_id (str): ID of the source database (daily performance)
            target_db_id (str, optional): ID of the target database (monthly summary)
            parent_page_id (str, optional): ID of the parent page for creating databases
        """
        self.source_db_id = source_db_id
        self.target_db_id = target_db_id
        self.parent_page_id = parent_page_id
    
    def fetch_all_records(self, database_id=None):
        """
        Fetch all records from a Notion database.
        
        Args:
            database_id (str, optional): Database ID to fetch from. 
                                      If not provided, uses source_db_id.
        
        Returns:
            list: List of all records from the database
        """
        if database_id is None:
            database_id = self.source_db_id
        
        if not database_id:
            raise ValueError("Database ID is required. Set it using set_database_ids() or pass it directly.")
        
        results = []
        response = self.client.databases.query(database_id=database_id)
        results.extend(response["results"])
        
        while response.get("has_more"):
            response = self.client.databases.query(
                database_id=database_id,
                start_cursor=response["next_cursor"]
            )
            results.extend(response["results"])
        
        return results
    
    def results_to_dataframe(self, results):
        """
        Convert Notion API results to a pandas DataFrame.
        
        Args:
            results (list): List of Notion API results
            
        Returns:
            pd.DataFrame: DataFrame with Date, Year, Month, Day, and rnd columns
        """
        rows = []
        
        for page in results:
            props = page.get("properties", {})
            
            # Extract Date and rnd values
            date_value = props.get("Date", {}).get("date", {}).get("start")
            rnd_value = props.get("rnd", {}).get("formula", {}).get("number")
            
            if date_value and rnd_value is not None:
                date_parsed = parser.parse(date_value)
                rows.append({
                    "Date": date_parsed,
                    "Year": date_parsed.year,
                    "Month": date_parsed.strftime("%B"),
                    "Day": date_parsed.strftime("%A"),
                    "rnd": rnd_value
                })
        
        return pd.DataFrame(rows)
    
    def get_daily_data(self, database_id=None):
        """
        Get daily habit data from a Notion database.
        
        Args:
            database_id (str, optional): Database ID to fetch from.
            
        Returns:
            pd.DataFrame: Daily habit data
        """
        results = self.fetch_all_records(database_id)
        return self.results_to_dataframe(results)
    
    def aggregate_monthly_performance(self, df):
        """
        Aggregate daily data into monthly averages.
        
        Args:
            df (pd.DataFrame): Daily habit data
            
        Returns:
            pd.DataFrame: Monthly aggregated data
        """
        # Step 1 — Aggregate
        monthly_avg = (
            df.groupby(["Year", "Month"], as_index=False)["rnd"]
              .mean()
              .rename(columns={"rnd": "avg_rnd"})
        )
        
        # Step 2 — Add numeric month value for proper sorting
        monthly_avg["Month_Num"] = monthly_avg["Month"].apply(
            lambda m: list(calendar.month_name).index(m)
        )
        
        # Step 3 — Sort by Year (desc), then Month (desc)
        monthly_avg = (
            monthly_avg
            .sort_values(["Year", "Month_Num"], ascending=[False, False])
            .drop(columns="Month_Num")
            .reset_index(drop=True)
        )
        
        # Step 4 — Round and return
        monthly_avg["avg_rnd"] = monthly_avg["avg_rnd"].round(2)
        
        return monthly_avg
    
    def get_or_create_monthly_summary_db(self, parent_page_id=None):
        """
        Get or create a monthly summary database.
        
        Args:
            parent_page_id (str, optional): Parent page ID. If not provided,
                                         uses the one set in set_database_ids().
        
        Returns:
            str: Database ID of the monthly summary database
        """
        if parent_page_id is None:
            parent_page_id = self.parent_page_id
        
        if not parent_page_id:
            raise ValueError("Parent page ID is required.")
        
        # Step 1 — List all blocks (children) inside the parent page
        blocks = self.client.blocks.children.list(parent_page_id)["results"]
        
        # Step 2 — Check for existing inline or child database
        for block in blocks:
            if block["type"] == "child_database":
                db_title = block["child_database"]["title"].strip().lower()
                if db_title == "monthly aggregate summary".lower():
                    print(f"✅ Found existing inline database: {db_title} ({block['id']})")
                    return block["id"]
        
        # Step 3 — If not found, create a new database
        db = self.client.databases.create(
            parent={"type": "page_id", "page_id": parent_page_id},
            title=[{"type": "text", "text": {"content": "Monthly Aggregate Summary"}}],
            icon={"type": "emoji", "emoji": "🧮"},
            properties={
                "Name": {"title": {}},
                "Average rnd": {"number": {"format": "number"}},
                "Date": {"date": {}},
            },
        )
        
        print(f"🆕 Created new Notion database with correct properties: {db['id']}")
        return db["id"]
    
    def upsert_monthly_summary(self, db_id, monthly_df):
        """
        Update or insert monthly summary records into a Notion database.
        
        Args:
            db_id (str): Database ID to update
            monthly_df (pd.DataFrame): Monthly aggregated data
        """
        # Step 1 — Get all existing pages in the database
        existing_pages = []
        response = self.client.databases.query(database_id=db_id)
        existing_pages.extend(response["results"])
        
        while response.get("has_more"):
            response = self.client.databases.query(
                database_id=db_id, start_cursor=response["next_cursor"]
            )
            existing_pages.extend(response["results"])
        
        # Step 2 — Build lookup of existing entries (by Name)
        existing_map = {}
        for page in existing_pages:
            title_prop = page["properties"]["Name"]["title"]
            if title_prop:
                name_label = title_prop[0]["plain_text"]
                existing_map[name_label] = page["id"]
        
        # Step 3 — Update or insert
        for _, row in monthly_df.iterrows():
            month_label = f"{row['Month']} {int(row['Year'])}"
            avg_value = float(row["avg_rnd"])
            
            # Represent the month as a Notion date (first of the month)
            month_number = datetime.strptime(row["Month"], "%B").month
            date_value = f"{row['Year']}-{month_number:02d}-01"
            
            if month_label in existing_map:
                # ✅ Update existing
                self.client.pages.update(
                    page_id=existing_map[month_label],
                    properties={
                        "Average rnd": {"number": avg_value},
                        "Date": {"date": {"start": date_value}},
                    },
                )
                print(f"🔄 Updated: {month_label} → {avg_value}")
            else:
                # ➕ Insert new entry
                self.client.pages.create(
                    parent={"database_id": db_id},
                    properties={
                        "Name": {"title": [{"text": {"content": month_label}}]},
                        "Average rnd": {"number": avg_value},
                        "Date": {"date": {"start": date_value}},
                    },
                )
                print(f"➕ Added: {month_label} → {avg_value}")
        
        print("\n✅ Monthly summary successfully updated in Notion!")
    
    def process_habit_tracking(self, source_db_id, parent_page_id):
        """
        Complete workflow for processing habit tracking data.
        
        This method:
        1. Fetches daily data from source database
        2. Converts to DataFrame
        3. Aggregates monthly performance
        4. Creates/updates monthly summary database
        
        Args:
            source_db_id (str): Source database ID
            parent_page_id (str): Parent page ID for summary database
            
        Returns:
            tuple: (daily_df, monthly_df, summary_db_id)
        """
        # Set database IDs
        self.set_database_ids(source_db_id, parent_page_id=parent_page_id)
        
        # Get daily data
        print("📊 Fetching daily habit data...")
        daily_df = self.get_daily_data()
        print(f"✅ Fetched {len(daily_df)} daily records")
        
        # Aggregate monthly performance
        print("📈 Aggregating monthly performance...")
        monthly_df = self.aggregate_monthly_performance(daily_df)
        print(f"✅ Created monthly aggregates for {len(monthly_df)} months")
        
        # Create/update summary database
        print("🗄️ Managing monthly summary database...")
        summary_db_id = self.get_or_create_monthly_summary_db()
        self.upsert_monthly_summary(summary_db_id, monthly_df)
        
        return daily_df, monthly_df, summary_db_id
