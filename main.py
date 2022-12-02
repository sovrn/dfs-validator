import os
import pyspark
import boto3
import json
import datetime
import time
import sys
from botocore.exceptions import ClientError

DATABASE = 'dd'

# get all device type counts
#select count(*), device_type from dd.weblog_superset_dt_2022092221 group by device_type;

#group by hour
#select dt_hour, count(*) from dd.weblog where device_type is null group by dt_hour;

#select from all
#select count(*) from dd.weblog where device_type is null;

#TODO: What do we actually want to limit it to?
#TODO: Do we want to randomize this? When do we want our check?

query = """
select count(*) from dd.weblog_superset_dt_2022092221 where device_type is null;
"""
output = "s3://aaa-dsol-test-cases/dfs-validator/validator-outputs/"

s3 = boto3.resource('s3')

client = boto3.client('athena', region_name='us-east-2')

def lambda_handler(event, context):
    #executes the query and returns {'QueryExecutionId': 'string'}
    response_query_execution_id = client.start_query_execution( # MAY require ClientRequestToken
        QueryString = query,
        QueryExecutionContext = {
            'Database' : DATABASE
        },
        ResultConfiguration={
            'OutputLocation' : output,
        }
    )

    
    response_get_query_details=client.get_query_execution(QueryExecutionId=response_query_execution_id['QueryExecutionId'])

    status = 'RUNNING'
    #check for status of query execution every second for 5 seconds. TODO: adjust this based on needs
    iterations = 5
    while(iterations > 0):
        iterations = iterations - 1
        #this takes query execution id as input and returns the details of the executed query
        response_get_query_details = client.get_query_execution(QueryExecutionId = response_query_execution_id['QueryExecutionId'])
        status = response_get_query_details['QueryExecution']['Status']['State'] #may be QueryExecutions. Might have to specify first result.
        print(status)
        if(status == 'FAILED') or (status == 'CANCELLED'):
            print('ansible job failed')
            return False
        elif status == 'SUCEEDED' :
            location = response_get_query_details['QueryExecution']['ResultConfiguration']['OutputLocation']
            #returns query results
            response_query_result = client.get_query_results(QueryExecutionId = response_query_execution_id['QueryExecutionId'])
            print('location ' + location)
            rowheaders = response_query_result['ResultSet']['Rows'][0]['Data'] #I think I should replace this with the data that I actually want to see.
            for row in response_query_result['ResultSet']['Rows']:
                print(row)

            return True
        else:
            time.sleep(1)