import  os
from    dotenv import load_dotenv, find_dotenv
from    enum   import Enum

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
# ======================================================================