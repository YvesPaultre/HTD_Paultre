# Assessment 4 Code Review - Yves Paltre
## Database Creation & Population

 - [x] SQL Server Created
 - [x] SQL Server Populated
 - [x] MongoDB Created
 - [x] MongoDb Populated
 - [x] Data Warehouse Created
 - [ ] Data Warehouse Populated
	 - [ ] All tables are empty
 - [ ] ETL Pipeline successfully executed
	 - [ ] No confirmation pipeline completed.
- [ ] Tests pass with 90% or more coverage.
	- [ ] 85% Coverage
- [ ] All Test pass
	- [ ] 5 of 46 tests failed

### INSTR FEEDBACK (SC):  
- 
- 
- Great answers to the questions.


#### `cleaning.py` 
- `clean_dates`function
	- The function is returning `datetime.date` objects, but the test expects string values in the format 'YYYY-MM-DD'.
- `clean_numerics` function
	- The code is using `np.NaN` which has been removed in NumPy 2.0. It should use `np.nan` (lowercase) instead.
	- The implementation is trying to modify DataFrame rows directly in a loop, which is inefficient and causing errors.
- `clean_text` function
	- The regex pattern has a flaw. The problem is in the regular expression pattern `r"[\w]"`:
		1. `\w` is a regex metacharacter that matches any word character (alphanumeric characters plus underscore).
		2. The pattern `[\w]` creates a character class containing this metacharacter.
		3. When used with `str.replace()`, this will __remove all alphanumeric characters and underscores__ from the text.		
		For example, if the input is "Hello, World!", after applying this regex it would become ", !" (all letters and numbers removed). This is likely the opposite of the intended behavior. Usually, text cleaning would:
		- Keep alphanumeric characters
		- Remove or replace special characters, extra whitespace, etc.
		The proper pattern might be `r"[^\w\s]"` (to remove non-word, non-whitespace characters) or a more specific pattern depending on the desired outcome.


#### `transformers.py`
- `transform_books` function
	- The function attempts to normalize the 'title' field but doesn't check if the column exists first. This is causing failures in the error branch tests where the test is providing a dummy DataFrame with only 'pub_date'.

#### `loaders.py`
- `get_table_columns` function
	- The tests expect a helper function called `get_table_columns` but an approach using SQLAlchemy's inspect was implmented, which is causing the `test_load_dimension_table` and  `test_load_fact_table` unit tests to fail.
	- Both unit tests have the error message: "does not have the attribute `get_table_columns`".

#### `test_etl_pipeline_error_branches`
- The test is expecting a 'validate error' but getting a 'transform error' because the transform_books function fails on KeyError for 'title' before the validation step can occur.
	- Modify the `clean_dates` function to return strings instead of datetime objects
	- Fix `clean_numerics` to use `np.nan` instead of `np.NaN` and use a vectorized approach
	- Add column existence checks in the transform functions
	- Implement the missing `get_table_columns` function or modify the tests
	- Ensure proper error propagation in the ETL pipeline

#### `data_quality.py`
- `check_duplicate` function Line 35
	- As written `df[field].value_counts(value)` - when you pass a specific value to `value_counts()`, it doesn't return the count of that value. Instead, it treats the input as a normalization method. This creates incorrect counting logic.
- `quality_report` function Line 74
	- The calculation for unique composition percentage is producing large negative numbers. The negative percentages (-83800.00%) indicate a mathematical error. The issue is likely that `total_duplicates` is being calculated incorrectly. When `duplicated()` is called on a dataframe with many similar values, the counts can become very large relative to the total row count.


INSTR: Scott Certain
