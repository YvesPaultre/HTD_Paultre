"""
Loaders for BookHaven ETL Assessment
"""
import pandas as pd
import sqlalchemy
import pyodbc
import config
import logging

logger = logging.getLogger(__name__)

# --- Load Dimension Table ---
def load_dimension_table(df: pd.DataFrame, table_name: str, sql_conn_str: str):
    """Load a dimension table into SQL Server.
    Hint: Use SQLAlchemy to create an engine and pandas.DataFrame.to_sql.
    Before loading, truncate (do not drop) the table, and filter your DataFrame to only columns that exist in the table schema.
    Use if_exists='append'. See 'Robust Loading Patterns' in the README.
    """

    engine = sqlalchemy.create_engine(sql_conn_str)
    try:
        with engine.begin() as conn:
            # Gather schema data
            column_names = [col for col in get_table_columns(table_name, engine) if 'key' not in col] # For dimension tables, strip keys from requirement
            
            # Limit data frame to columns in table
            df = df[column_names]
            
            # Truncate table - Must use DELETE FROM due to foreign key constraints, and must first ensure fact table has been truncated:
            conn.execute(sqlalchemy.text(f"TRUNCATE TABLE fact_book_sales")) # Obnoxious but necessary. This WOULD be a problem in a more complex or professional pipeline.
            conn.execute(sqlalchemy.text(f"DELETE FROM {table_name}"))

            # Load new data into table
            rows_affected = df.to_sql(table_name, conn, if_exists='append', index=False, chunksize=config.BATCH_SIZE)
    
            return rows_affected
    except Exception as e:
        logger.error(f"Encountered unexpected exception while inserting into table {table_name}: {e}")


# --- Load Fact Table ---
def load_fact_table(df: pd.DataFrame, table_name: str, sql_conn_str: str):
    """Load a fact table into SQL Server.
    Hint: Use SQLAlchemy and pandas.DataFrame.to_sql. Ensure referential integrity with dimension tables.
    Before loading, truncate (do not drop) the table, and filter your DataFrame to only columns that exist in the table schema.
    Use if_exists='append'. See 'Robust Loading Patterns' in the README.
    """
    engine = sqlalchemy.create_engine(sql_conn_str)
    try:
        with engine.begin() as conn:
            # Gather schema data
            column_names = [col for col in get_table_columns(table_name, engine) if not col == 'sales_key']
            # Limit data frame to columns in table
            df = df[column_names]

            # Truncate table
            conn.execute(sqlalchemy.text(f"TRUNCATE TABLE {table_name}"))

            # Load new data into table
            rows_affected = df.to_sql(table_name, conn, if_exists='append', index=False, chunksize=config.BATCH_SIZE)
    
            return rows_affected
    except Exception as e:
        logger.error(f"Encountered unexpected exception while inserting into table {table_name}: {e}")

# Dummy method- needed for testing
def get_table_columns(table_name, engine):
    schema_data = sqlalchemy.inspect(engine)
    columns = schema_data.get_columns(table_name)
    column_names = []
    for c in columns:
        column_names.append(c['name'])
    return column_names