"""
Transformers for BookHaven ETL Assessment
"""
import pandas as pd
from etl import extractors # Use this for easier order transformation

# --- Book Series Transformer ---
def transform_book_series(df_books):
    """Add a 'series_normalized' column (copy of 'series' for now)."""
    df_books = df_books.copy()
    if 'series' in df_books.columns:
        df_books['series_normalized'] = df_books['series']
    else:
        df_books['series_normalized'] = None
    return df_books

# --- Author Collaborations Transformer ---
def transform_author_collaborations(df_authors):
    """Add a 'collab_count' column (number of collaborations)."""
    df_authors = df_authors.copy()
    if 'collaborations' in df_authors.columns:
        df_authors['collab_count'] = df_authors['collaborations'].apply(lambda x: len(x) if isinstance(x, (list, tuple)) else 0)
    else:
        df_authors['collab_count'] = 0
    return df_authors

# --- Customer Reading History Transformer ---
def transform_reading_history(df_customers):
    """Add a 'reading_count' column (number of books read)."""
    df_customers = df_customers.copy()
    if 'reading_history' in df_customers.columns:
        df_customers['reading_count'] = df_customers['reading_history'].apply(lambda x: len(x) if isinstance(x, (list, tuple)) else 0)
    else:
        df_customers['reading_count'] = 0
    return df_customers

# --- Book Recommendations Transformer ---
def transform_book_recommendations(df_books, df_customers):
    """Return books DataFrame with a dummy 'recommended_score' column."""
    df_books = df_books.copy()
    df_books['recommended_score'] = 1.0  # Placeholder
    return df_books

# --- Genre Preferences Transformer ---
def transform_genre_preferences(df_customers):
    """Add a 'num_genres' column (number of preferred genres)."""
    df_customers = df_customers.copy()
    if 'genre_preferences' in df_customers.columns:
        df_customers['num_genres'] = df_customers['genre_preferences'].apply(lambda x: len(x) if isinstance(x, (list, tuple)) else 0)
    else:
        df_customers['num_genres'] = 0
    return df_customers

# Transformation module for BookHaven ETL (STUDENT VERSION)
"""Business logic and star schema transformation functions.

Instructions:
- Implement each function to transform the input DataFrame for loading into the star schema.
- Apply business rules, SCD Type 1 logic, and any required joins or aggregations (see 'ETL Transformations with Pandas').
- Ensure output matches the target schema for each dimension/fact table.
- Document your approach and any edge cases handled.
"""
def transform_books(books_df: pd.DataFrame) -> pd.DataFrame:
    """Transform books data for star schema loading.
    Hint: Normalize fields, handle series/recommendations, and ensure all required columns are present. See 'ETL Transformations with Pandas'.
    """
    # Normalize title (Proper Case)
    books_df['title'] = books_df['title'].str.strip().str.title()
    # Normalize author (Proper Case)
    books_df['author'] = books_df['author'].str.strip().str.title()
    # Normalize genre (Proper Case)
    books_df['genre'] = books_df['genre'].str.strip().str.title()
    # Normalize ISBN
    # Format rules: 10-digit pre-2007, 13-digit post
    # Strip hyphens, as these are inconsistent in formatting/separation
    # Re-insert hyphen before last digit (checksum digit is consistent)
    books_df['isbn'] = books_df['isbn'].str.replace('-', '').apply(lambda x: x[:len(x)-1] + '-' + x[len(x)-1:])
    # Normalize Series (Proper Case)
    books_df['series'] = books_df['series'].str.strip().str.title() # Provided series function is incompatible with SQL schema.
    books_df = transform_book_recommendations(books_df, None) # Unnecessary argument in function signature. Wonder where that might have come from?

    return books_df

def transform_authors(authors_df: pd.DataFrame):
    """Transform authors data for star schema loading.
    Hint: Handle collaborations, normalize genres, and ensure all required columns are present. See 'ETL Transformations with Pandas'.
    """
    # Normalize name (Proper Case)
    authors_df['name'] = authors_df['name'].str.strip().str.title()
    # Email should already be normalized by cleaning process
    # Phone should already be normalized by cleaning process
    # Normalize genres
    authors_df['genres'] = authors_df['genres'].transform(lambda x: ', '.join(map(str, x)))
    # Handle collaborations
    # Note that this is useless for loading into the warehouse, as the warehouse has no column for collaborations
    authors_df['collaborations'] = authors_df['collaborations'].transform(lambda x: ', '.join(map(str, x)))

    return authors_df

def transform_customers(customers_df: pd.DataFrame):
    """Transform customers data for star schema loading.
    Hint: Flatten reading history, genre preferences, and recommendations. See 'ETL Transformations with Pandas'.
    """
    # Normalize name
    customers_df['name'] = customers_df['name'].str.strip().str.title()
    # Email should already be normalized by cleaning process
    # Phone should already be normalized by cleaning process (and deleted in a lot of cases, apparently)
    # Normalize reading history
    # Apply provided transform function FIRST
    customers_df = transform_reading_history(customers_df)
    customers_df['reading_history'] = customers_df['reading_history'].transform(lambda x: ', '.join(map(str, x)))
    # Normalize genre preferences
    # Apply provided transform function FIRST
    customers_df = transform_genre_preferences(customers_df)
    customers_df['genre_preferences'] = customers_df['genre_preferences'].transform(lambda x: ', '.join(map(str, x)))
    # Normalize recommendations
    
    customers_df['recommendations'] = customers_df['recommendations'].transform(lambda x: ', '.join(map(str, x))) # Also clean the ISBNs while we're at it
    
    return customers_df

def transform_orders(orders_df: pd.DataFrame, book_author_lookup: pd.DataFrame):
    """Transform orders data for star schema loading.
    Hint: Join with dimension keys as needed. See 'ETL Transformations with Pandas' and 'Star Schema Design'.
    """
    # Gather other relevant tables
    authors_df = extractors.extract_sqlserver_table('dim_author', 'sql_server_dw')
    books_df = extractors.extract_sqlserver_table('dim_book', 'sql_server_dw')
    customer_df = extractors.extract_sqlserver_table('dim_customer', 'sql_server_dw')
    dates_df = extractors.extract_sqlserver_table('dim_date', 'sql_server_dw')

    # Rename columns in orders_df to match dimension tables
    orders_df = orders_df.rename(columns={"book_isbn": "isbn", "order_date": "date"})

    # Replicate standardizations
    orders_df['isbn'] = orders_df['isbn'].str.replace('-', '').apply(lambda x: x[:len(x)-1] + '-' + x[len(x)-1:])

    orders_df = ( # Perform merge
        orders_df.merge(books_df, 'left', on='isbn') # Contains book key
        .merge(book_author_lookup, 'left', on='isbn') # Allows us to merge author
        .merge(authors_df, 'left', on='name') # Contains author key
        .merge(customer_df, 'left', on='customer_id') # Contains customer key
        .merge(dates_df, 'left', on='date') # Contains date key
    )

    return orders_df[['book_key', 'author_key', 'customer_key', 'date_key', 'quantity', 'price']]
    