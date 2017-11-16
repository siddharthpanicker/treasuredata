# Python Script to Connect to Treasure Data API and execute a query
# import libs
import tdclient
import argparse
import urllib3

# Disable warnings for SSL
urllib3.disable_warnings()

# Define API Key
apikey = "9522/d2903e9b47426bf5c55bd44ba6da7888b8968ba6"

# Define Arguments
parser = argparse.ArgumentParser()
parser.add_argument("--db_name", help="Required: Database name")
parser.add_argument("--table_name", help="Required: Table name")
parser.add_argument("--col_list", help="Optional: Comma separated list of columns. If not specified all columns are selected")
parser.add_argument("--min_time", help="Optional: Unix timestamp or 'NULL'")
parser.add_argument("--max_time", help="Optional: Unix timestamp and should be greater than min time or 'NULL'")
parser.add_argument("--query_engine", help="Optional: Query Engine - Acceptable values are 'hive' or 'presto'. Defaults to 'presto'")
parser.add_argument("--output_format", help="Optional: Output Format - Acceptable values are ‘csv’ or ‘tabular'. Defaults to ‘tabular’'")
parser.add_argument("--query_limit", help="Optional: Limit the number of rows displayed. Default is 100")

# Check argument validity
args = parser.parse_args()
check_arguments_status=0
allowed_query_engine = ['hive','presto']
allowed_query_output_format = ['tabular','csv']
if not args.db_name:
    print("Database Name is required and must be specified")
    check_arguments_status=1
if not args.table_name:
    print("Table Name is required and must be specified")
    check_arguments_status=1
if args.query_engine and not args.query_engine in allowed_query_engine:
    print("Query Engine Invalid. Allowed Values are 'hive' or 'presto'")
    check_arguments_status=1
if args.output_format and not args.output_format in allowed_query_output_format:
    print("Output Format Invalid. Allowed Values are 'tabular' or 'csv'")
    check_arguments_status=1
if check_arguments_status == 1:
    exit(1)

# Define Query, Database and other parameters

user_query='SELECT * FROM party'
database_name='sid_td_test'

# Execute Query and Display result
with tdclient.Client(apikey) as client:
    job = client.query(database_name, user_query)
    # sleep until job's finish
    job.wait()
    for row in job.result():
        print(row)
