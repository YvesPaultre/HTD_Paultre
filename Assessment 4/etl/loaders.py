"""
Loaders for BookHaven ETL Assessment
"""
import pandas as pd
import sqlalchemy
import pyodbc
import config
import logging

logger = logging.getLogger('__name__')

# --- Load Dimension Table ---
def load_dimension_table(df: pd.DataFrame, table_name, sql_conn_str):
    """Load a dimension table into SQL Server.
    Hint: Use SQLAlchemy to create an engine and pandas.DataFrame.to_sql.
    Before loading, truncate (do not drop) the table, and filter your DataFrame to only columns that exist in the table schema.
    Use if_exists='append'. See 'Robust Loading Patterns' in the README.
    """

    engine = sqlalchemy.create_engine(sql_conn_str)
    try:
        with engine.begin() as conn:
            # Gather schema data
            schema_data = sqlalchemy.inspect(engine)
            columns = schema_data.get_columns(table_name)
            column_names = []
            for c in columns:
                column_names.append(c['name'])
            
            # Limit data frame to columns in table
            df = df[column_names]

            # Truncate table
            conn.execute(sqlalchemy.text("TRUNCATE TABLE :table_name"), table_name)

            # Load new data into table
            rows_affected = df.to_sql(table_name, conn, if_exists='append', index=False, chunksize=config.BATCH_SIZE)
    
            return rows_affected
    except Exception as e:
        logger.error(f"Encountered unexpected exception while inserting into table {table_name}: {e}")


# --- Load Fact Table ---
def load_fact_table(df, table_name, sql_conn_str):
    """Load a fact table into SQL Server.
    Hint: Use SQLAlchemy and pandas.DataFrame.to_sql. Ensure referential integrity with dimension tables.
    Before loading, truncate (do not drop) the table, and filter your DataFrame to only columns that exist in the table schema.
    Use if_exists='append'. See 'Robust Loading Patterns' in the README.
    """
    engine = sqlalchemy.create_engine(sql_conn_str)
    try:
        with engine.begin() as conn:
            # Gather schema data
            schema_data = sqlalchemy.inspect(engine)
            columns = schema_data.get_columns(table_name)
            column_names = []
            for c in columns:
                column_names.append(c['name'])
            
            # Limit data frame to columns in table
            df = df[column_names]

            # Truncate table
            conn.execute(sqlalchemy.text("TRUNCATE TABLE :table_name"), table_name)

            # Load new data into table
            rows_affected = df.to_sql(table_name, conn, if_exists='append', index=False, chunksize=config.BATCH_SIZE)
    
            return rows_affected
    except Exception as e:
        logger.error(f"Encountered unexpected exception while inserting into table {table_name}: {e}")