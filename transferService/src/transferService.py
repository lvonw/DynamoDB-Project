import os
import boto3
import psycopg
import constants


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

# List all tables in DynamoDB
table_names = client.list_tables()["TableNames"]
print(table_names)

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

        # ========== ACCESS PATTERN 1 ==================================

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
        cur.execute("""
                    SELECT fa.actor_id, film_id, first_name, last_name 
                    FROM film_actor AS fa
                    INNER JOIN actor AS a
                    ON fa.actor_id = a.actor_id""")
        results = cur.fetchall()

        # TODO Höhere Ebene, damit mann die anzahl der schauspieler sehen kann
        for record in results:
            table.put_item(
                Item={
                    constants.TABLE_PARTITION_KEY:  "ACT#" + str(record[0]),
                    constants.TABLE_SORTING_KEY:    "FLM#" + str(record[1]),
                    "first_name": str(record[2]),
                    "last_name": str(record[3])})


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



        # ==============================================================
        conn.commit()


        # ========== TESTS =============================================
        print("========== TESTS =============================================")
        test(4581, a4, "Gesamtanzahl der verfügbaren Filme")
        test((759, 762), b4, "Anzahl der Unterschiedlichen Filem je Standort")
        test(4582, 0, "Gesamtanzahl der verfügbaren Filme")
        test(4582, 0, """Die Vor- und Nachnamen der 10 Schauspieler mit den 
             meisten Filmen, absteigend sortiert.""")

        

