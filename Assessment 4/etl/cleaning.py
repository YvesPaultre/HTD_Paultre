"""
Data cleaning module for BookHaven ETL Assessment (Student Version)

Instructions:
- Implement each function to clean and standardize the specified field in the DataFrame.
- Use pandas string/date methods and regular expressions as needed (see 'Data Quality & Cleaning with Pandas').
- Handle missing, invalid, or inconsistent data as described in the lessons.
- Document your approach and any edge cases handled.
"""
import pandas as pd
import logging
import numpy as np
import re
from datetime import datetime

logger = logging.getLogger(__name__)

# --- Clean Dates ---
def clean_dates(df: pd.DataFrame, field: str) -> pd.DataFrame:
    """Clean and standardize date fields to YYYY-MM-DD format.
    Hint: Use pandas.to_datetime with error handling. See 'Data Quality & Cleaning with Pandas'.
    """
    try:
        # Not sure why the test calls for converting to datetime and then BACK to string, but go off I guess.
        df[field] = pd.to_datetime(df[field], format="%Y-%m-%d", errors='coerce').dt.strftime('%Y-%m-%d') 
    except ValueError:
        logger.warning(f"Unable to parse all values in column \"{field}\" to datetime.")
    except TypeError:
        logger.warning(f"Unable to parse all values in column \"{field}\" to datetime.")
    
    return df

# --- Clean Emails ---
def clean_emails(df: pd.DataFrame, field: str) -> pd.DataFrame:
    """Clean and validate email fields (set invalid emails to None or NaN). (NaN fails tests, use None.)
    Hint: Use regular expressions and pandas apply. See 'Data Quality & Cleaning with Pandas' and 'Unit Testing for Data Transformations'.
    """
    email_pattern = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
    
    try:
        df[field] = df[field].apply(
            func=lambda x: x if x and email_pattern.match(x) else None # If x is empty or does not match pattern, set to NaN
        )
    except TypeError as e:
        logger.error(f"Unable to parse all values in column {field}: {e}")

    return df 

# --- Clean Phone Numbers ---
def clean_phone_numbers(df: pd.DataFrame, field: str) -> pd.DataFrame:
    """Standardize phone numbers (remove non-digits, set invalid to None).
    Hint: Use regular expressions and pandas string methods. See 'Data Quality & Cleaning with Pandas'.
    """
    phone_pattern = re.compile(r"[^\d]")
    try:
        if field in df.columns:
            df[field] = df[field].str.strip().str.replace(phone_pattern, "", regex=True).apply( # Strip whitespace, then remove all non-numeric characters, chain into apply
                func=lambda x: x if x and len(x) == 10 else None # Format: (xxx) xxx-xxxx if 10-digit number, else invalid
            ) # Removed additional formatting as this fails tests. Format segment: f"({x[:3]}) {x[3:6]}-{x[6:]}" (replaces x in lambda)
    except TypeError as e:
        logger.error(f"Unable to parse all values in column {field}: {e}")

    return df

# --- Clean Numerics ---
def clean_numerics(df: pd.DataFrame, field: str) -> pd.DataFrame:
    """Convert to numeric, set invalid to NaN.
    Hint: Use pandas.to_numeric with error handling. See 'Data Quality & Cleaning with Pandas'.
    """

    try:
        df[field] = pd.to_numeric(df[field], errors='coerce')
    except ValueError:
        logger.warning(f"Unable to convert column \"{field}\" to numeric, setting all values to NaN")
        df[field] = np.nan
    except TypeError:
        logger.warning(f"Unable to convert column \"{field}\" to numeric, setting all values to NaN")
        df[field] = np.nan

    return df

# --- Clean Text ---
def clean_text(df: pd.DataFrame, field: str) -> pd.DataFrame:
    """Clean text fields (e.g., strip whitespace, standardize case, remove special characters).
    Hint: Use pandas string methods. See 'Pandas Fundamentals for ETL' and 'Data Quality & Cleaning with Pandas'.
    """
    try:
        df[field] = df[field].str.strip().str.replace(r"[^\w\s.,!]", "").str.capitalize() if field in df.columns else pd.NA # Using NA flags for dropna(), using None doesn't necessarily guarantee it'll be dropped
    except TypeError as e:
        logger.error(f"Unable to convert column \"{field}\" to standard format: {e}")

    return df

# --- Remove Duplicates ---
def remove_duplicates(df: pd.DataFrame, subset=None):
    """Remove duplicate rows based on subset of fields.
    Hint: Use pandas.drop_duplicates. See 'Data Quality & Cleaning with Pandas'.
    """
    return df.drop_duplicates(subset=subset)

# --- Handle Missing Values ---
def handle_missing_values(df: pd.DataFrame, strategy='drop', fill_value=None):
    """Handle missing values using specified strategy ('drop', 'fill').
    Hint: Use pandas.dropna or pandas.fillna. See 'Data Quality & Cleaning with Pandas'.
    """
    match strategy:
        case 'drop':
            df.dropna(inplace=True)
        case 'fill':
            df.fillna(fill_value, inplace=True)
        case _:
            logger.error("Invalid strategy option specified, canceling operation")

    return df