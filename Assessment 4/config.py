# BookHaven ETL Assessment - Configuration
from pathlib import Path

DATABASE_CONFIG = {
    'sql_server_source': {
        'server': 'localhost',
        'database': 'BookHavenSource',
        'username': 'sa',
        'password': 'yourStrong(!)Password',
        'port': 1433
    },
    'sql_server_dw': {
        'server': 'localhost',
        'database': 'BookHavenDW',
        'username': 'sa',
        'password': 'yourStrong(!)Password',
        'port': 1433
    },
    'mongodb': {
        'connection_string': 'mongodb://localhost:27017/',
        'database': 'bookhaven_customers'
    }
}

FILE_CONFIG = {
    'csv_file_source': str(Path(__file__).parent / 'data' / 'csv' / 'book_catalog.csv'),
    'json_file_source': str(Path(__file__).parent / 'data' / 'json' / 'author_profiles.json')
}

# ETL Pipeline Options
BATCH_SIZE = 1000
QUALITY_THRESHOLD = 90  # Minimum data quality score (0-100)
LOG_LEVEL = 'INFO'

# Add more configuration options as needed 
BOOK_RULES = {
    'title': {'required': True},
    'author': {'required': True},
    'genre': {'required': True},
    'isbn': {'required': True}
}
AUTHOR_RULES = {
    'name': {'required': True},
    'email': {'required': True}
}
CUSTOMER_RULES = {
    'name': {'required': True},
    'email': {'required': True}
}
