import  os
import  boto3
import  psycopg
import  constants

from    tqdm        import tqdm

def log (msg):
    print(GREEN + msg + CLEAR)

def test(expected, result, desc):
    passed = expected == result
    c = GREEN if passed else RED
    ext = " PASSED! " if passed else " FAILED! "
    print(CLEAR + "TESTING: " + 
          BLUE + desc + CLEAR)
    print("EXPECTED: " + 
          BLUE + str(expected) + CLEAR + 
          " GOT: " + 
          c + str(result) + ext + c )
    print()
    

table = constants.TABLE
client = constants.DYNAMO_DB_CLIENT

RED     = "\033[91m"
GREEN   = "\033[92m"
BLUE    = "\033[94m"
CLEAR   = "\033[0m"

# ======================================================================
# As we decided to follow the best practice, and recommendation of the 
# DynamoDB documentation we utilize the single table design scheme.
# Further, the documentation for data modelling for DynamoDB advises to
# model the data to best suit the access patterns for it, rather than to
# reflect the relations between the items as one might find in a RDB. 
# To this end we defined our access patterns as the tasks layed out by
# the assignment and designed our database accordingly.
# ======================================================================
with psycopg.connect(dbname     = constants.POSTGRES_DB_NAME, 
                     user       = constants.POSTGRES_DB_USER, 
                     password   = constants.POSTGRES_DB_PASSWORD,
                     host       = constants.POSTGRES_DB_HOST,
                     port       = constants.POSTGRES_DB_PORT) as conn:

    # Open a cursor to perform database operations
    with conn.cursor() as cur:
        print("========== CREATE ============================================")
        # ========== ACCESS PATTERN 1 ==================================
        # As we use a single table approach, it would be inefficient to
        # scan through each partition key, to see which rows are actual
        # inventory items. Therefore we employ a master partition which
        # allows us to query all movies with a single partition key.

        # cur.execute("SELECT inventory_id FROM inventory")
        # results = cur.fetchall()

        # for record in results:
        #     table.put_item(
        #         Item={
        #             constants.TABLE_PARTITION_KEY:  "MINV#1",
        #             constants.TABLE_SORTING_KEY:    "INV#" + str(record[0])})

        # ========== ACCESS PATTERN 2 ==================================
        # cur.execute("SELECT store_id, film_id FROM inventory")
        # results = cur.fetchall()

        # for record in results:
        #     table.put_item(
        #         Item={
        #             constants.TABLE_PARTITION_KEY:  "STR#" + str(record[0]),
        #             constants.TABLE_SORTING_KEY:    "FLM#" + str(record[1])})

        # ========== ACCESS PATTERN 3 ==================================
        # We again use a master partition to be able to query all actors
        # without looking through all partition keys, and then use a 
        # composite sorting key to associate an actor with a movie they
        # made. As we havent really found a good way to sort based on 
        # amount of occurence in Dynamo, we will have to do this later 
        # in the client code.  

        # log("Querying data from postgres for access pattern 3")
        # cur.execute("""
        #             SELECT fa.actor_id, film_id, first_name, last_name 
        #             FROM film_actor AS fa
        #             INNER JOIN actor AS a
        #             ON fa.actor_id = a.actor_id""")
        # results = cur.fetchall()

        # log("Inserting into DynamoDB")
        # for record in results:
        #     table.put_item(
        #         Item={
        #             constants.TABLE_PARTITION_KEY:  "MACT#1",
        #             constants.TABLE_SORTING_KEY:    "ACT#" + str(record[0]) +
        #                                             "FLM#" + str(record[1]),
        #             "first_name": str(record[2]),
        #             "last_name": str(record[3])})
            
        # log("Done\n")

        # ========== ACCESS PATTERN 4 ==================================
        # Associate staff with payments via secondary index. We do it in
        # this fashion, because this allows us to do the same with the 
        # customers in Access Pattern 7. 
        log("Querying data from postgres for access pattern 4")
        cur.execute("""
                    SELECT payment_id, staff_id, amount
                    FROM payment """)
        results = cur.fetchall()

        log("Inserting into DynamoDB")
        # table.update(
        #     AttributeDefinitions=[
        #         {"AttributeName": "GSI-1-PK", "AttributeType": "S"},
        #         {"AttributeName": "GSI-1-SK", "AttributeType": "S"},
        #         {"AttributeName": "amount",   "AttributeType": "N"}],
        #     GlobalSecondaryIndexUpdates=[{ 
        #         "Create": {
        #             "IndexName" : "GSI-1",
        #             "KeySchema" : [
        #                 {"AttributeName" : "GSI-1-PK", "KeyType" : "HASH"},
        #                 {"AttributeName" : "GSI-1-SK", "KeyTyps" : "RANGE"}],
        #             "Projection" : {
        #                 "ProjectionType" : "ALL"
        #             }
        #         }
        #     }]
        # )
        
        # put_items = []
        # for record in results:
        #     item = {
        #         constants.TABLE_PARTITION_KEY:  "MPAY#1",
        #         constants.TABLE_SORTING_KEY:    "PAY#" + str(record[0]),
        #         "GSI-1-PK"  : "MSTF#1",
        #         "GSI-1-SK"  : "STF#" + str(record[1]),
        #         "amount"    : record[2]}
        #     put_items.append({"PutRequest": {"Item": item}})
        
        # # Batch write items
        # with table.batch_writer() as batch:
        #     for item in put_items:
        #         batch.put_item(Item=item["PutRequest"]["Item"])

        # log("Done\n")
        # ========== ACCESS PATTERN 5 ==================================
        # ========== ACCESS PATTERN 6 ==================================
        # ========== ACCESS PATTERN 7 ==================================
        # ========== ACCESS PATTERN 8 ==================================
        # ========== ACCESS PATTERN 9 ==================================
        print("========== READ ==============================================")

        # ========== 4.A ===============================================
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key(
                constants.TABLE_PARTITION_KEY).eq("MINV#1"),
            Select="COUNT"
        )
        a4 = response["Count"]

        # ========== 4.B ===============================================
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key(
                constants.TABLE_PARTITION_KEY).eq("STR#1"),
            Select="COUNT"
        )
        b41 = response["Count"]
        
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key(
                constants.TABLE_PARTITION_KEY).eq("STR#2"),
            Select="COUNT"
        )
        b42 = response["Count"]

        b4 = (b41, b42)

        # ========== 4.C ===============================================
        # ========== 4.D ===============================================
        response = table.query(
            IndexName="GSI-1",
            KeyConditionExpression=
                boto3.dynamodb.conditions.Key("GSI-1-PK").eq("MSTF#1") &
                boto3.dynamodb.conditions.Key("GSI-1-SK").eq("STF#2")
        )
        print(response["Count"])

        response = table.query(
            IndexName="GSI-1",
            KeyConditionExpression=
                boto3.dynamodb.conditions.Key("GSI-1-PK").eq("MSTF#1") &
                boto3.dynamodb.conditions.Key("GSI-1-SK").eq("STF#1")
        )
        print(response["Count"])

        response = table.query(
            IndexName="GSI-1",
            KeyConditionExpression=
                boto3.dynamodb.conditions.Key("GSI-1-PK").eq("MSTF#1")
        )

        print(response["ConsumedCapacity"])

        d4 = {}
        for item in response["Items"]:
            if item["GSI-1-SK"] in d4: 
                d4[item["GSI-1-SK"]] += item["amount"]   
            else:
                d4[item["GSI-1-SK"]] = item["amount"] 
        print(d4)
        # ========== 4.E ===============================================
        # ========== 4.F ===============================================
        # ========== 4.G ===============================================
        # ========== 4.H ===============================================
        # ========== 4.I ===============================================




        # ==============================================================
        conn.commit()


        # ========== TESTS =============================================
        print("========== TESTS =============================================")
        test(4581, a4, "Gesamtanzahl der verfügbaren Filme")
        test((759, 762), b4, "Anzahl der Unterschiedlichen Filem je Standort")
        test(4582, 0, "Gesamtanzahl der verfügbaren Filme")
        test(4582, 0, """Die Vor- und Nachnamen der 10 Schauspieler mit den 
             meisten Filmen, absteigend sortiert.""")

        

