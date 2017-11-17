
# coding: utf-8

# In[ ]:

# Python Script to Connect to Treasure Data API and execute a query

# import libs

import tdclient
import argparse
import urllib3
import csv
import prettytable
from prettytable import PrettyTable


# Set CSV file path and location for csv output

csvfile="C:\ProgramData\Anaconda3\sampleoutput.csv"

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
parser.add_argument("--query_engine", help="Optional: Query Engine - Acceptable values are 'hive' or 'presto'. Defaults to 'hive'")
parser.add_argument("--output_format", help="Optional: Output Format - Acceptable values are ‘csv’ or ‘tabular'. Defaults to ‘tabular’'")
parser.add_argument("--query_limit", help="Optional: Limit the number of rows displayed. Default is 100")

# Set allowed values for arguments and other variables

check_arguments_status=0
allowed_query_engine = ['hive','presto']
allowed_query_output_format = ['tabular','csv']
row_count = 0
i = 0

# Parse Arguments and check for validity

args = parser.parse_args()
if not args.db_name:
    print('\n',"Database Name is required and must be specified")
    check_arguments_status=1
if not args.table_name:
    print('\n',"Table Name is required and must be specified")
    check_arguments_status=1
if args.query_engine and not args.query_engine in allowed_query_engine:
    print('\n',"Query Engine Invalid. Allowed Values are 'hive' or 'presto'")
    check_arguments_status=1
if args.output_format and not args.output_format in allowed_query_output_format:
    print('\n',"Output Format Invalid. Allowed Values are 'tabular' or 'csv'")
    check_arguments_status=1
if not args.col_list:
    args.col_list = '*'
if args.query_limit:
    args.query_limit = ' LIMIT ' + args.query_limit

# Exit program if arguments are invalid

if check_arguments_status == 1:
    exit(1)

# Build Treasure Data Query

database_name=args.db_name
if (args.min_time == 'NULL' or not args.min_time) and args.max_time != 'NULL':
    user_query= 'SELECT ' +  args.col_list + ' FROM ' + args.table_name + ' WHERE TD_TIME_RANGE(time, NULL , ' + '"' + args.max_time + '")' + args.query_limit 
if (args.max_time == 'NULL' or not args.max_time) and args.min_time != 'NULL':
    user_query= 'SELECT ' +  args.col_list + ' FROM ' + args.table_name + ' WHERE TD_TIME_RANGE(time, ' + '"' + args.min_time + '"' + ', NULL) ' + args.query_limit 
if (not args.max_time or args.max_time == 'NULL') and (not args.min_time or args.min_time == 'NULL'):
    user_query= 'SELECT ' +  args.col_list + ' FROM ' + args.table_name + args.query_limit 
if args.min_time != 'NULL' and args.max_time != 'NULL':
    user_query= 'SELECT ' +  args.col_list + ' FROM ' + args.table_name + ' WHERE TD_TIME_RANGE(time, ' + '"' + args.min_time + '",'  + '"' + args.max_time + '")' + args.query_limit 
    
print('\n',"The query you submitted is:",'\n\n',user_query)


# Execute Query and return results

try:
    with tdclient.Client(apikey) as client:
        job = client.query(database_name, user_query)
        
        # sleep until job's finish
        job.wait()
        
        # Create two lists to copy result schema array of arrays and then copy only column names as header into flattened array.
        result_elements = []
        result_header = []
        
        # Copy array of arrays from job result schema
        for row in job.result_schema:
            result_elements.append(row)
        
        # Copy column names from array of arrays into flattened array
        while i < len(result_elements):
            result_header.append(result_elements[i][0])
            i = i + 1
        
        # If output format specified is blank or tabular, display the data using pretty table
        
        if args.output_format == 'tabular' or not args.output_format:
            t = PrettyTable(result_header)
            for row in job.result():
                t.add_row(row)
                row_count = row_count + 1
            # Check for empty resultset before displaying output
            if row_count != 0:
                print(t)
        
        # If output format specified is csv, output the data to csv file
        
        if args.output_format == 'csv': 
            print('\n','CSV output format selected')
            print('\n','Writing output to ',csvfile)
            with open(csvfile, "w") as output:
                writer = csv.writer(output, lineterminator='\n')
                writer.writerow(result_header)
                for row in job.result():
                    writer.writerow(row) 
                    row_count = row_count + 1

# Catch Exceptions

except tdclient.errors.NotFoundError:
    print('\n',"Database does not exist. Check database name.")
except TypeError:
    print('\n',"Table does not exist. Check table name.")

# Check for empty resultset
if row_count == 0:
    print('\n',"Query did not return any data.")

exit(0)
    

