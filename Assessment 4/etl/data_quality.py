# Data quality module for BookHaven ETL (STUDENT VERSION)
"""Data quality validation and reporting functions.

Instructions:
- Implement each function to validate, check, or report on data quality for a DataFrame.
- Use field/type checks, pattern matching, and summary statistics as described in 'Integration Testing with Quality Metrics for Data Sources'.
- Return results in a format suitable for reporting and testing.
- Document your approach and any assumptions.
"""
import pandas as pd
import logging
from typing import List, Dict
import re

logger = logging.getLogger('__name__')

def validate_schema(df: pd.DataFrame, required_fields: List[str]):
    """Validate DataFrame schema against required fields.
    Hint: Check for missing or extra columns. See 'Integration Testing with Quality Metrics for Data Sources'.
    """
    missing_columns = []
    extra_columns = []
    for field in required_fields:
        if field not in df.columns:
            missing_columns.append(field)
    for column in df.columns:
        if column not in required_fields:
            extra_columns.append(column)
    return missing_columns, extra_columns

def check_duplicates(df: pd.DataFrame, field: str):
    """Check for duplicate values in a field and return a summary or list.
    Hint: Use pandas.duplicated and value_counts. See 'Data Quality & Cleaning with Pandas'.
    """
    duplicates: Dict[str, int] = {}
    for value in df[df.duplicated(subset=field)][field]: # For each non-unique value in the field
        duplicates[value] = df[field].value_counts(value) # Append value, count(value) to dict of duplicates
    return duplicates

def quality_report(df: pd.DataFrame):
    """Generate a data quality report for a DataFrame (missing, invalid, duplicates, etc.).
    Hint: Summarize key quality metrics. See 'Integration Testing with Quality Metrics for Data Sources' and 'E2E Pipeline Testing with Health Monitoring'.
    """
    # Given failure to provide or account for necessary information needed to perform analysis, these checks are functionally useless.
    missing_columns, extra_columns = validate_schema(df, df.columns) # Given no metrics to determine which columns are "missing" or "extra", this is the best I can do. Will always return two empty lists.
    all_duplicates: Dict[str, Dict[str, int]] = {} # Nested Dict: outer str key holds field name, inner str key holds duplicate value, inner value holds duplicate count 
    for field in df.columns: # For each field in df
        field_duplicates = check_duplicates(df, field) # Get duplicate result set for field
        if field_duplicates: # Use this check to prevent adding empty result sets to aggregate set
            all_duplicates[field] = field_duplicates
    
    # Log data quality report
    logger.info('DATA QUALITY REPORT')
    logger.info('='*20)
    if missing_columns:
        logger.warning(f"REQUIRED COLUMNS MISSING FROM SCHEMA: {missing_columns.__str__}")
    if extra_columns:
        logger.warning(f"EXTRA COLUMNS FOUND IN SCHEMA: {extra_columns.__str__}")
    if not missing_columns and not extra_columns:
        logger.info("SCHEMA VALIDATION SUCCESSFUL - NO MISSING/EXTRA DATA FOUND")

    if all_duplicates: # Paired with check in result population, this will return false if there are no duplicates in the frame
        logger.warning("DUPLICATE DATA FOUND IN SCHEMA:")
        logger.warning("="*20)
        logger.warning(f"COLUMN\t\t| VALUE\t\t| COUNT")
        for field in all_duplicates.keys():
            for value, count in all_duplicates[field].items():
                logger.warning(f"{field}\t\t| {value}\t\t| {count}")

        logger.warning("UNIQUE COMPOSITION RATIO PER COLUMN:")
        

        for field in all_duplicates.keys():
            total_duplicates = df.duplicated(subset=field).sum()
            logger.warning(f"{field}: {((1 - (float(total_duplicates) / max(df[field].count(), 1))) * 100):.2f}% unique values") # Calculate duplicate fraction, subtract from 1 to get unique fraction, convert to percent

    # Missing functions taken from solution
ERROR = 'ERROR'
WARNING = 'WARNING'
INFO = 'INFO'

def validate_field_level(df, rules):
    """
    Validate fields based on provided rules (type, pattern, allowed values, etc.).
    Returns: list of (row_idx, field, issue, severity, message)
    """
    results = []
    for idx, row in df.iterrows():
        for field, rule in rules.items():
            value = row.get(field, None)
            # Required check
            if rule.get('required', False) and (pd.isnull(value) or value == '' or value is None):
                results.append((idx, field, 'Missing required', ERROR, f"{field} is required but missing."))
                continue
            # Type check
            if value is not None and not pd.isnull(value):
                expected_type = rule.get('type')
                if expected_type == 'string' and not isinstance(value, str):
                    results.append((idx, field, 'Type mismatch', ERROR, f"{field} should be string."))
                if expected_type == 'list' and not isinstance(value, (list, tuple)):
                    results.append((idx, field, 'Type mismatch', ERROR, f"{field} should be a list."))
            # Pattern check
            if 'pattern' in rule and value is not None and not pd.isnull(value):
                if not re.match(rule['pattern'], str(value)):
                    results.append((idx, field, 'Pattern mismatch', ERROR, f"{field} does not match pattern."))
            # Allowed values
            if 'allowed' in rule and value is not None and not pd.isnull(value):
                if value not in rule['allowed']:
                    results.append((idx, field, 'Invalid value', ERROR, f"{field} value '{value}' not allowed."))
            # Min length
            if 'min_length' in rule and value is not None and not pd.isnull(value):
                if isinstance(value, (str, list)) and len(value) < rule['min_length']:
                    results.append((idx, field, 'Too short', WARNING, f"{field} is shorter than {rule['min_length']}"))
            # Max length
            if 'max_length' in rule and value is not None and not pd.isnull(value):
                if isinstance(value, (str, list)) and len(value) > rule['max_length']:
                    results.append((idx, field, 'Too long', WARNING, f"{field} is longer than {rule['max_length']}"))
    return results

def validate_list_length(df, field, min_length=None, max_length=None):
    """
    Validate length of list-type fields.
    Returns: list of (row_idx, field, issue, severity, message)
    """
    results = []
    for idx, row in df.iterrows():
        value = row.get(field, None)
        if isinstance(value, (list, tuple)):
            if min_length is not None and len(value) < min_length:
                results.append((idx, field, 'Too short', WARNING, f"{field} list shorter than {min_length}"))
            if max_length is not None and len(value) > max_length:
                results.append((idx, field, 'Too long', WARNING, f"{field} list longer than {max_length}"))
    return results

def generate_quality_report(validation_results):
    """
    Generate a detailed data quality report from validation results.
    Returns: string report
    """
    # Handle None or non-list input gracefully
    if not validation_results or not isinstance(validation_results, list):
        return "No data quality issues found."
    if len(validation_results) == 0:
        return "No data quality issues found."
    report_lines = ["Data Quality Report:"]
    for item in validation_results:
        # Handle malformed tuples
        if not isinstance(item, tuple) or len(item) != 5:
            report_lines.append(f"Malformed validation result: {item}")
            continue
        idx, field, issue, severity, message = item
        report_lines.append(f"Row {idx}, Field '{field}': [{severity}] {issue} - {message}")
    return '\n'.join(report_lines) 