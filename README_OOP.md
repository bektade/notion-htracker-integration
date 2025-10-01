# Notion Habit Tracker - OOP Implementation

This project has been refactored to use Object-Oriented Programming (OOP) principles, making it more modular, reusable, and maintainable.

## 🏗️ Architecture

### Core Class: `Notion`

The main class that handles all Notion database operations for habit tracking.

**Location**: `modules/notion_client.py`

### Key Features

- **Encapsulation**: All Notion API interactions are contained within the class
- **Reusability**: Can be used with any Notion database by simply changing IDs
- **Modularity**: Each operation is a separate method
- **Error Handling**: Built-in validation and error messages

## 🚀 Quick Start

### Basic Usage

```python
from modules.notion_client import Notion

# Create Notion client
notion = Notion()

# Process habit tracking data (one-line solution)
daily_df, monthly_df, summary_db_id = notion.process_habit_tracking(
    source_db_id="your-source-db-id",
    parent_page_id="your-parent-page-id"
)
```

### Step-by-Step Usage

```python
from modules.notion_client import Notion

# Create client
notion = Notion()

# Set database IDs
notion.set_database_ids(
    source_db_id="your-source-db-id",
    parent_page_id="your-parent-page-id"
)

# Get daily data
daily_df = notion.get_daily_data()

# Aggregate monthly performance
monthly_df = notion.aggregate_monthly_performance(daily_df)

# Create/update summary database
summary_db_id = notion.get_or_create_monthly_summary_db()
notion.upsert_monthly_summary(summary_db_id, monthly_df)
```

## 📋 Class Methods

### Initialization
- `__init__(notion_token=None)`: Initialize with Notion API token

### Database Management
- `set_database_ids(source_db_id, target_db_id=None, parent_page_id=None)`: Set database IDs
- `fetch_all_records(database_id=None)`: Fetch all records from a database
- `get_daily_data(database_id=None)`: Get daily habit data as DataFrame

### Data Processing
- `results_to_dataframe(results)`: Convert Notion results to pandas DataFrame
- `aggregate_monthly_performance(df)`: Aggregate daily data into monthly averages

### Summary Database Operations
- `get_or_create_monthly_summary_db(parent_page_id=None)`: Get or create summary database
- `upsert_monthly_summary(db_id, monthly_df)`: Update or insert monthly summaries

### Complete Workflow
- `process_habit_tracking(source_db_id, parent_page_id)`: Complete end-to-end processing

## 🔧 Configuration

### Environment Variables

Create a `.env` file with your Notion token:

```env
NOTION_TOKEN=your_notion_integration_token_here
```

### Database IDs

Update the database IDs in your script:

```python
SOURCE_DB_ID = "your-source-database-id"
PARENT_PAGE_ID = "your-parent-page-id"
```

## 📊 Data Flow

1. **Fetch**: Get daily records from source database
2. **Transform**: Convert to pandas DataFrame with proper date handling
3. **Aggregate**: Calculate monthly averages and sort by date
4. **Store**: Create/update summary database with monthly data

## 🎯 Benefits of OOP Implementation

### Before (Procedural)
- Hard-coded database IDs
- Mixed concerns in single file
- Difficult to reuse for different databases
- No error handling

### After (OOP)
- ✅ Reusable for any Notion database
- ✅ Clean separation of concerns
- ✅ Built-in error handling and validation
- ✅ Easy to extend and maintain
- ✅ One-line complete workflow

## 📁 File Structure

```
notion-htracker-integration/
├── main.py                    # Simplified main script
├── modules/
│   ├── notion_client.py      # OOP Notion class
│   ├── toDataFrame.py        # Legacy (now integrated)
│   └── updateDatabase.py     # Legacy (now integrated)
├── example_usage.py          # Usage examples
└── README_OOP.md             # This documentation
```

## 🔄 Migration from Legacy Code

### Old Way
```python
from modules.toDataFrame import notion_results_to_df
from modules.updateDatabase import get_or_create_monthly_summary_db, upsert_monthly_summary

# Multiple steps with hard-coded values
results = notion.databases.query(database_id=SOURCE_DB_ID)
df = notion_results_to_df(results)
# ... more manual steps
```

### New Way
```python
from modules.notion_client import Notion

notion = Notion()
daily_df, monthly_df, summary_db_id = notion.process_habit_tracking(
    source_db_id=SOURCE_DB_ID,
    parent_page_id=PARENT_PAGE_ID
)
```

## 🧪 Testing

Run the example usage to test the implementation:

```bash
python example_usage.py
```

## 🔮 Future Enhancements

The OOP structure makes it easy to add:

- Multiple database support
- Custom aggregation methods
- Data validation
- Caching mechanisms
- Async operations
- Custom export formats

## 📝 Notes

- The legacy modules (`toDataFrame.py`, `updateDatabase.py`) are still available but integrated into the class
- All original functionality is preserved
- The new implementation is backward compatible
- Environment variables are automatically loaded
