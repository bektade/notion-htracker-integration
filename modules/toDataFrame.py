# modules/toDf.py

import pandas as pd
from dateutil import parser

def notion_results_to_df(results):
    """
    Converts a Notion API results list into a clean pandas DataFrame.

    Adds Date, Year, Month, Day (weekday), and rnd columns.
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
                "Day": date_parsed.strftime("%A"),   # 👈 Add weekday name
                "rnd": rnd_value
            })

    # Create and return DataFrame
    df = pd.DataFrame(rows)
    return df
