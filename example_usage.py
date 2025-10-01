"""
Example usage of the Notion OOP client for habit tracking.

This file demonstrates different ways to use the Notion class for various scenarios.
"""

from modules.notion_client import Notion

def example_basic_usage():
    """Basic usage example - complete workflow in one call."""
    print("=== Basic Usage Example ===")
    
    # Create Notion client
    notion = Notion()
    
    # Configuration
    source_db_id = "27fb645e06f880a6aa40c479b93cb235"
    parent_page_id = "27fb645e06f880b7aae4cb1a6602473d"
    
    # Process everything in one call
    daily_df, monthly_df, summary_db_id = notion.process_habit_tracking(
        source_db_id=source_db_id,
        parent_page_id=parent_page_id
    )
    
    print(f"✅ Processed {len(daily_df)} daily records")
    print(f"✅ Created {len(monthly_df)} monthly aggregates")
    print(f"✅ Summary database ID: {summary_db_id}")


def example_step_by_step_usage():
    """Step-by-step usage example for more control."""
    print("\n=== Step-by-Step Usage Example ===")
    
    # Create Notion client
    notion = Notion()
    
    # Set database IDs
    notion.set_database_ids(
        source_db_id="27fb645e06f880a6aa40c479b93cb235",
        parent_page_id="27fb645e06f880b7aae4cb1a6602473d"
    )
    
    # Step 1: Get daily data
    print("📊 Fetching daily data...")
    daily_df = notion.get_daily_data()
    print(f"✅ Fetched {len(daily_df)} daily records")
    
    # Step 2: Aggregate monthly performance
    print("📈 Aggregating monthly performance...")
    monthly_df = notion.aggregate_monthly_performance(daily_df)
    print(f"✅ Created {len(monthly_df)} monthly aggregates")
    
    # Step 3: Create/update summary database
    print("🗄️ Managing summary database...")
    summary_db_id = notion.get_or_create_monthly_summary_db()
    notion.upsert_monthly_summary(summary_db_id, monthly_df)
    
    return daily_df, monthly_df, summary_db_id


def example_custom_database_usage():
    """Example using custom database IDs."""
    print("\n=== Custom Database Usage Example ===")
    
    # Create Notion client with custom token
    notion = Notion()  # Will use NOTION_TOKEN from environment
    
    # Use different databases
    custom_source_db = "your-custom-source-db-id"
    custom_parent_page = "your-custom-parent-page-id"
    
    try:
        # This will work with any valid Notion database
        daily_df, monthly_df, summary_db_id = notion.process_habit_tracking(
            source_db_id=custom_source_db,
            parent_page_id=custom_parent_page
        )
        print("✅ Custom database processing successful!")
        return daily_df, monthly_df, summary_db_id
    except Exception as e:
        print(f"❌ Error with custom database: {e}")
        return None, None, None


def example_data_analysis():
    """Example of analyzing the data after processing."""
    print("\n=== Data Analysis Example ===")
    
    notion = Notion()
    
    # Get data
    daily_df, monthly_df, _ = notion.process_habit_tracking(
        source_db_id="27fb645e06f880a6aa40c479b93cb235",
        parent_page_id="27fb645e06f880b7aae4cb1a6602473d"
    )
    
    # Analyze data
    print("📊 Data Analysis:")
    print(f"   Total daily records: {len(daily_df)}")
    print(f"   Date range: {daily_df['Date'].min()} to {daily_df['Date'].max()}")
    print(f"   Average rnd score: {daily_df['rnd'].mean():.2f}")
    print(f"   Best day: {daily_df.loc[daily_df['rnd'].idxmax(), 'Date']} ({daily_df['rnd'].max()})")
    print(f"   Monthly aggregates: {len(monthly_df)} months")
    
    # Show top performing months
    print("\n🏆 Top 3 months:")
    top_months = monthly_df.head(3)
    for _, row in top_months.iterrows():
        print(f"   {row['Month']} {row['Year']}: {row['avg_rnd']}")


if __name__ == "__main__":
    # Run examples
    example_basic_usage()
    example_step_by_step_usage()
    example_data_analysis()
    
    # Uncomment to test with custom databases
    # example_custom_database_usage()
