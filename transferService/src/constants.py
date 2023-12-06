import  os
import  boto3
from    dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

# ========== POSTGRES ==================================================
POSTGRES_DB_NAME        = "dvdrental"
POSTGRES_DB_HOST        = "localhost"
POSTGRES_DB_PORT        = "5432"
POSTGRES_DB_USER        = os.environ["DB_USER_POSTGRES"]
POSTGRES_DB_PASSWORD    = os.environ["DB_PASSWORD_POSTGRES"]
# ======================================================================

# ========== DYNAMO ====================================================
DYNAMO_DB_RESOURCE_NAME     = "dynamodb" 
DYNAMO_DB_HOST              = "http://localhost:8000"
DYNAMO_DB_REGION_NAME       = "us-west-2"
DYNAMO_DB_ACCESS_KEY_ID     = os.environ["DB_ACCESS_KEY_ID_DYNAMO"]
DYNAMO_DB_SECRET_ACCESS_KEY = os.environ["DB_SECRET_ACCESS_KEY_DYNAMO"]

DYNAMO_DB_CLIENT            = boto3.client(
    DYNAMO_DB_RESOURCE_NAME, 
    endpoint_url            = DYNAMO_DB_HOST,
    region_name             = DYNAMO_DB_REGION_NAME,
    aws_access_key_id       = DYNAMO_DB_ACCESS_KEY_ID,
    aws_secret_access_key   = DYNAMO_DB_SECRET_ACCESS_KEY)
# ======================================================================

# ========== TABLE =====================================================
TABLE_NAME                  = "dvdrental"
TABLE_PARTITION_KEY         = "partition_key" 
TABLE_SORTING_KEY           = "sorting_key" 
TABLE_BILLING_MODE          = "PAY_PER_REQUEST"

TABLE_ATTRIBUTE_DEFINITIONS = attribute_definitions = [
    {"AttributeName": TABLE_PARTITION_KEY,  "AttributeType": "S"},
    {"AttributeName": TABLE_SORTING_KEY,    "AttributeType": "S"}
]

TABLE_KEY_SCHEMA            = [
    {"AttributeName": TABLE_PARTITION_KEY,  "KeyType": "HASH"},
    {"AttributeName": TABLE_SORTING_KEY,    "KeyType": "RANGE"}
]

# __TABLE_RESP                = DYNAMO_DB_CLIENT.create_table(
#     TableName               = TABLE_NAME,
#     AttributeDefinitions    = TABLE_ATTRIBUTE_DEFINITIONS,
#     KeySchema               = TABLE_KEY_SCHEMA,
#     BillingMode             = TABLE_BILLING_MODE)

TABLE                       = boto3.resource(
    DYNAMO_DB_RESOURCE_NAME, 
    endpoint_url            = DYNAMO_DB_HOST,
    region_name             = DYNAMO_DB_REGION_NAME,
    aws_access_key_id       = DYNAMO_DB_ACCESS_KEY_ID,
    aws_secret_access_key   = DYNAMO_DB_SECRET_ACCESS_KEY).Table(TABLE_NAME)
# ======================================================================


