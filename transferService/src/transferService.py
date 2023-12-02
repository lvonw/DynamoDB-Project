import os
import boto3
import psycopg
import constants

dynamodb = boto3.client(
    constants.DYNAMO_DB_RESOURCE_NAME, 
    endpoint_url           = constants.DYNAMO_DB_HOST,
    region_name            = constants.DYNAMO_DB_REGION_NAME,
    aws_access_key_id      = constants.DYNAMO_DB_ACCESS_KEY_ID,
    aws_secret_access_key  = constants.DYNAMO_DB_SECRET_ACCESS_KEY)

table_name = 'your_table_name'

key_schema = [
    {'AttributeName': 'your_primary_key', 'KeyType': 'HASH'},
    # Add more key attributes if needed
]

attribute_definitions = [
    {'AttributeName': 'your_primary_key', 'AttributeType': 'S'},
    # Add more attribute definitions if needed
]

table = dynamodb.create_table(TableName = table_name,
                              AttributeDefinitions = attribute_definitions,
                              KeySchema=key_schema,
                              BillingMode="PAY_PER_REQUEST")

# List all tables in DynamoDB
table_names = dynamodb.list_tables()["TableNames"]
print(table_names)
# with psycopg.connect(dbname     = constants.POSTGRES_DB_NAME, 
#                      user       = constants.POSTGRES_DB_USER, 
#                      password   = constants.POSTGRES_DB_PASSWORD,
#                      host       = constants.POSTGRES_DB_HOST,
#                      port       = constants.POSTGRES_DB_PORT) as conn:

    # # Open a cursor to perform database operations
    # with conn.cursor() as cur:

    #     cur.execute("SELECT * FROM actor")
    #     results = cur.fetchall()

    #     print(cur.rowcount)

    #     ##for record in results:
    #     ##    print(record)
            
    #     cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    #     # Fetch all the results
    #     tables = cur.fetchall()
    #     # Iterate over the tables and print their names
    #     for table in tables:
    #         print(table[0])


    #     conn.commit()

