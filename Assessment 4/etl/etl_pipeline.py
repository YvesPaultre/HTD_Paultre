# Main ETL Pipeline for BookHaven ETL (STUDENT VERSION)
"""
Main entry point for the BookHaven ETL pipeline.

Instructions:
- Implement the ETL pipeline by calling each step in order: extract, clean, validate, transform, load, and report.
- Use the modular functions you implemented in the other ETL modules.
- Add logging, error handling, and SLA/performance tracking as described in 'E2E Pipeline Testing with Health Monitoring'.
- Reference the milestone checklist and rubric in the README.
- Document your approach and any assumptions.
"""
from etl import extractors, cleaning, data_quality, transformers, loaders
import config # Change from importing specifically DATABASE_CONFIG to allow for access to other config items
import logging
import pandas as pd

def main():
    """Run the ETL pipeline (students must implement each step).
    Hint: Follow the ETL workflow from the lessons. Use try/except for error handling and log/report each step's results.
    """
    logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("etl_pipeline.log"), logging.StreamHandler()],
    )
    logger = logging.getLogger(__name__)
    
    # 1. Extract data from all sources (see extractors.py)
    logger.info("Attempting extraction from all sources...")
    try:
        csv_path = config.FILE_CONFIG['csv_file_source']
        book_catalog = extractors.extract_csv_book_catalog(csv_path) # Returns data frame
    except Exception as e:
        logger.error(f"Encountered an error importing from CSV: {e}")
        raise Exception("extract error")

    try:
        json_path = config.FILE_CONFIG['json_file_source']
        author_profiles = extractors.extract_json_author_profiles(json_path)
    except Exception as e:
        logger.error(f"Encountered an error importing from JSON: {e}")
        raise Exception("extract error")

    try:
        mongo_cfg = config.DATABASE_CONFIG['mongodb']
        mongo_customers = extractors.extract_customers_from_mongodb(mongo_cfg['connection_string'], mongo_cfg['database'])
    except Exception as e:
        logger.error(f"Encountered error importing from MongoDB: {e}")
        raise Exception("extract error")
        
    try:
        sql_customers = extractors.extract_sqlserver_table('customers', 'sql_server_source')
    except Exception as e:
        logger.error(f"Encountered error importing customers from SQL server: {e}")
        raise Exception("extract error")

    try:
        orders = extractors.extract_sqlserver_table('orders', 'sql_server_source')
    except Exception as e:
        logger.error(f"Encountered error importing orders from SQL server: {e}")
        raise Exception("extract error")

    # 2. Clean and validate data (see cleaning.py, data_quality.py)
    logger.info("Attempting data cleaning...")
    # Note: No need to clean sql_customers as they will have already been validated by the SQL server

    try:
        # Clean emails
        author_profiles = cleaning.clean_emails(author_profiles, 'email')
        mongo_customers = cleaning.clean_emails(mongo_customers, 'email')
    except Exception as e:
        logger.error(f"Encountered an error during cleaning: {e}")
        raise Exception("clean error")
    
    try:
        # Clean phones
        author_profiles = cleaning.clean_phone_numbers(author_profiles, 'phone')
        mongo_customers = cleaning.clean_phone_numbers(mongo_customers, 'phone')
    except Exception as e:
        logger.error(f"Encountered an error during cleaning: {e}")
        raise Exception("clean error")
    
    try:
        # Clean dates
        book_catalog = cleaning.clean_dates(book_catalog, 'pub_date')
        orders = cleaning.clean_dates(orders, 'order_date')
    except Exception as e:
        logger.error(f"Encountered an error during cleaning: {e}")
        raise Exception("clean error")
    
    try:
        # Clean text
        author_profiles = cleaning.clean_text(author_profiles, 'bio')
        book_catalog = cleaning.clean_text(book_catalog, 'title')
    except Exception as e:
        logger.error(f"Encountered an error during cleaning: {e}")
        raise Exception("clean error")        
        # No raw numeric fields to clean
    
    try:
        # Assign customer ids
        new_id_start = sql_customers['customer_id'].max() + 1 if 'customer_id' in sql_customers.columns else 1
        mongo_customers.insert(0, 'customer_id', range(new_id_start, new_id_start + len(mongo_customers)))
    except Exception as e:
        logger.error(f"Encountered an error during cleaning: {e}")
        raise Exception("clean error")
    
    try:
        # Remove all missing data (treat all fields as required, as function signatures do not allow for ways to only evaluate specific columns.)
        book_catalog = cleaning.handle_missing_values(book_catalog, strategy='drop')
        author_profiles = cleaning.handle_missing_values(author_profiles, strategy='drop')
        mongo_customers = cleaning.handle_missing_values(mongo_customers, strategy='drop')
    except Exception as e:
        logger.error(f"Encountered an error during cleaning: {e}")
        raise Exception("clean error")
    # 3. Transform data for star schema (see transformers.py)
    # Each main transform method calls all relevant helper methods in logical order
    try:
        book_catalog = transformers.transform_books(book_catalog)
        author_profiles = transformers.transform_authors(author_profiles)
        mongo_customers = transformers.transform_customers(mongo_customers)

        # Construct date frame from dates in data
        dates = [] # Empty list
        for date in pd.date_range('1970-01-01', '2025-05-31'):
            dates.append([date.to_pydatetime().strftime('%Y-%m-%d'), int(date.to_pydatetime().strftime('%Y%m%d')), date.year, date.month, date.day])
        date_df = pd.DataFrame(dates, columns=['date', 'date_key', 'year', 'month', 'day'])
    except Exception as e:
        logger.error(f"Encountered an error during transforming: {e}")
        raise Exception("transform error")

    # Duplicate removal must come AFTER transform phase
    try:
        book_catalog = cleaning.remove_duplicates(book_catalog)
        author_profiles = cleaning.remove_duplicates(author_profiles)
        mongo_customers = cleaning.remove_duplicates(mongo_customers)
    except Exception as e:
        logger.error(f"Encountered an error during cleaning: {e}")
        raise Exception("clean error")
    
    # Validation
    try:
        book_validity = data_quality.validate_field_level(book_catalog, config.BOOK_RULES)
        author_validity = data_quality.validate_field_level(author_profiles, config.AUTHOR_RULES)
        sql_customer_validity = data_quality.validate_field_level(sql_customers, config.CUSTOMER_RULES)
        mongo_customer_validity = data_quality.validate_field_level(mongo_customers, config.CUSTOMER_RULES)
    except Exception as e:
        logger.error(f"Encountered an error during validation: {e}")
        raise Exception("validate error")

    # 4. Load data into SQL Server (see loaders.py)
    try:
        DW_CONNECTION_STRING = extractors.get_sql_server_conn_str('sql_server_dw')
        books_inserted = loaders.load_dimension_table(book_catalog, 'dim_book', DW_CONNECTION_STRING)
        authors_inserted = loaders.load_dimension_table(author_profiles, 'dim_author', DW_CONNECTION_STRING)
        # Append mongo customers to sql customers
        all_customers = pd.concat([sql_customers, mongo_customers])
        customers_inserted = loaders.load_dimension_table(all_customers, 'dim_customer', DW_CONNECTION_STRING)
        dates_inserted = loaders.load_dimension_table(date_df, 'dim_date', DW_CONNECTION_STRING)
    except Exception as e:
        logger.error(f"Encountered an error during loading phase 1: {e}")
        raise Exception("sql load error")

    # 3.5: Must transform orders AFTER loading dimensions, or else there are no keys to retrieve.
    # Generate book-author lookup frame by merging book_catalog with author_profiles and limiting to isbn and name columns
    try:
        book_catalog_to_merge = book_catalog.rename(columns={'author': 'name'})
        book_author_lookup = book_catalog_to_merge.merge(author_profiles, how='left', on='name')[['name', 'isbn']]
        orders = transformers.transform_orders(orders, book_author_lookup)
    except Exception as e:
        logger.error(f"Encountered an error during transforming phase 2: {e}")
        raise Exception("transform error")
    
    # 4.5: NOW we can load the fact table
    try:
        orders_inserted = loaders.load_fact_table(orders, 'fact_book_sales', DW_CONNECTION_STRING)
    except Exception as e:
        logger.error(f"Encountered an error during loading phase 2: {e}")
        raise Exception("sql load error")
    
    # 5. Output health/trend report (see README and lessons on monitoring)    
    logger.info("FINAL DATA METRICS: ")
    logger.info(f"{books_inserted} book records inserted successfully")
    logger.info(f"{authors_inserted} author records inserted successfully")
    logger.info(f"{customers_inserted} customer records inserted successfully")
    logger.info(f"{dates_inserted} dates inserted successfully")
    logger.info(f"{orders_inserted} book sale records inserted")
    data_quality.quality_report(book_catalog)
    data_quality.quality_report(author_profiles)
    data_quality.quality_report(all_customers)
    data_quality.quality_report(orders)

if __name__ == "__main__":
    main() 