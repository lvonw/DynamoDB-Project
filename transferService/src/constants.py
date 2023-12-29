import  os
import  time
import  boto3

from    dotenv  import load_dotenv, find_dotenv


load_dotenv(find_dotenv())

# ========== DEV =======================================================
CREATE_DYNAMOBD_TABLES  = True
RUN_TESTS               = False

REDUCE_TABLE_ROWS       = False # Good for testing as transfer takes a long time
REDUCTION_GOAL = 300 # Might not be reached due to foreign key constraints
TABLES_TO_REDUCE = [ 
        'payment', 'rental', 'customer', 'inventory',
        'film','address', 'city'
    ]
ASSOCIATION_TABLES_TO_REDUCE = [
        ('film_actor', 'actor_id', 'film_id'),
        ('film_category', 'film_id', 'category_id')
    ]
# ======================================================================

# ========== POSTGRES ==================================================
POSTGRES_DB_NAME        = "dvdrental"
POSTGRES_DB_HOST        = "postgresdb"
POSTGRES_DB_PORT        = "5432"
POSTGRES_DB_USER        = os.environ["DB_USER_POSTGRES"]
POSTGRES_DB_PASSWORD    = os.environ["DB_PASSWORD_POSTGRES"]
# ======================================================================

# ========== DYNAMO ====================================================
DYNAMO_DB_RESOURCE_NAME     = "dynamodb"
DYNAMO_DB_HOST              = "http://dynamodb-local:8000"
DYNAMO_DB_REGION_NAME       = "us-west-2"
DYNAMO_DB_ACCESS_KEY_ID     = os.environ["DB_ACCESS_KEY_ID_DYNAMO"]
DYNAMO_DB_SECRET_ACCESS_KEY = os.environ["DB_SECRET_ACCESS_KEY_DYNAMO"]

DYNAMO_DB_CLIENT            = boto3.client(
    DYNAMO_DB_RESOURCE_NAME, 
    endpoint_url            = DYNAMO_DB_HOST,
    region_name             = DYNAMO_DB_REGION_NAME,
    aws_access_key_id       = DYNAMO_DB_ACCESS_KEY_ID,
    aws_secret_access_key   = DYNAMO_DB_SECRET_ACCESS_KEY)

DYNAMO_DB_LOCAL_PATH        = "../../deploy/dynamodb/shared-local-instance.db"
# ======================================================================

# Delete dynamodb for recreation
if CREATE_DYNAMOBD_TABLES:
    # Delete tables
    tables = DYNAMO_DB_CLIENT.list_tables()['TableNames']
    for table_name in tables:
        DYNAMO_DB_CLIENT.delete_table(TableName=table_name)

    # Wait on table deletion
    time.sleep(5)

# ========== TABLE =====================================================
TABLE_NAME                  = 'dvdrental'
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

if CREATE_DYNAMOBD_TABLES:
    __TABLE_RESP                = DYNAMO_DB_CLIENT.create_table(
        TableName               = TABLE_NAME,
        AttributeDefinitions    = TABLE_ATTRIBUTE_DEFINITIONS,
        KeySchema               = TABLE_KEY_SCHEMA,
        BillingMode             = TABLE_BILLING_MODE)

TABLE                       = boto3.resource(
    DYNAMO_DB_RESOURCE_NAME, 
    endpoint_url            = DYNAMO_DB_HOST,
    region_name             = DYNAMO_DB_REGION_NAME,
    aws_access_key_id       = DYNAMO_DB_ACCESS_KEY_ID,
    aws_secret_access_key   = DYNAMO_DB_SECRET_ACCESS_KEY).Table(TABLE_NAME)
# ======================================================================

# ========== LOGGING ===================================================
LOG_DATE_FORMAT             = '%Y-%m-%d %H:%M:%S'
LOG_FILE_TRANSFER_SERVICE   = 'transferService.log'
# ======================================================================