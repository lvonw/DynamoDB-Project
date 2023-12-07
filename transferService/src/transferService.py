import  re
import  boto3
import  psycopg
import  constants
import  uuid

from    decimal     import Decimal
from    tqdm        import tqdm

def log (msg):
    print(GREEN + msg + CLEAR)

table = constants.TABLE
client = constants.DYNAMO_DB_CLIENT

RED     = "\033[91m"
GREEN   = "\033[92m"
BLUE    = "\033[94m"
CLEAR   = "\033[0m"

# ========== PREFACE ===================================================
# As we decided to follow the best practice, and recommendation of the 
# DynamoDB documentation we utilize the single table design scheme.
# Further, the documentation for data modelling in DynamoDB advises to
# model the data to best suit the access patterns for it, rather than to
# reflect the relations between the items as one might find in an RDB. 
# To this end we defined our access patterns as the tasks layed out by
# the assignment and designed our database accordingly.
# ======================================================================
with psycopg.connect(dbname     = constants.POSTGRES_DB_NAME, 
                     user       = constants.POSTGRES_DB_USER, 
                     password   = constants.POSTGRES_DB_PASSWORD,
                     host       = constants.POSTGRES_DB_HOST,
                     port       = constants.POSTGRES_DB_PORT) as conn:

    with conn.cursor() as cur:
        
        print("========== CREATE ============================================")
        # ========== ACCESS PATTERN 1 ==================================
        # As we use a single table approach, it would be inefficient to
        # scan through each partition key, to see which rows are actual
        # inventory items. Therefore we employ a master partition which
        # allows us to query all movies with a single partition key.
        
        # log("Querying data from postgres for access pattern 1")
        # cur.execute("SELECT inventory_id FROM inventory")
        # results = cur.fetchall()

        # log("Inserting into DynamoDB")
        # for record in tqdm(results):
        #     table.put_item(
        #         Item={
        #             constants.TABLE_PARTITION_KEY:  "MINV#1",
        #             constants.TABLE_SORTING_KEY:    "INV#" + str(record[0])})
        
        # log("Done\n")

        # ========== ACCESS PATTERN 2 ==================================
        
        # log("Querying data from postgres for access pattern 2")
        # cur.execute("SELECT store_id, film_id FROM inventory")
        # results = cur.fetchall()

        # log("Inserting into DynamoDB")
        # for record in tqdm(results):
        #     table.put_item(
        #         Item={
        #             constants.TABLE_PARTITION_KEY:  "STR#" + str(record[0]),
        #             constants.TABLE_SORTING_KEY:    "FLM#" + str(record[1])})
            
        # log("Done\n")

        # ========== ACCESS PATTERN 3 ==================================
        # We again use a master partition to be able to query all actors
        # without looking through all partition keys. We use composite
        # sorting keys to associate a movie with an actor.
        # To avoid data redundancy we then create another entry under 
        # the master actor partition. This time only with the actor id 
        # as the sorting key in order to recieve the first and last name

        # log("Querying data from postgres for access pattern 3")
        # cur.execute("""
        #             SELECT fa.actor_id, film_id, first_name, last_name 
        #             FROM film_actor AS fa
        #             INNER JOIN actor AS a
        #             ON fa.actor_id = a.actor_id""")
        # results = cur.fetchall()

        # log("Inserting into DynamoDB")
        # for record in tqdm(results):
        #     table.put_item(
        #         Item={
        #             constants.TABLE_PARTITION_KEY:  "MACT#1",
        #             constants.TABLE_SORTING_KEY:    "ACT#" + str(record[0]) +
        #                                             "FLM#" + str(record[1])})
        #     table.put_item(
        #         Item={
        #             constants.TABLE_PARTITION_KEY:  "MACT#1",
        #             constants.TABLE_SORTING_KEY:    "ACT#" + str(record[0]),
        #             "first_name": str(record[2]),
        #             "last_name": str(record[3])})
        
        # log("Done\n")

        # ========== ACCESS PATTERN 4 ==================================
        # Associate staff with payments via secondary index. We do it in
        # this fashion, because this allows us to do the same with the 
        # customers in Access Pattern 7. 

        # log("Querying data from postgres for access pattern 4")
        # cur.execute("""
        #             SELECT payment_id, staff_id, amount
        #             FROM payment """)
        # results = cur.fetchall()

        # log("Inserting into DynamoDB")
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
        #                 {"AttributeName" : "GSI-1-SK", "KeyType" : "RANGE"}],
        #             "Projection" : {
        #                 "ProjectionType" : "ALL"}
        #         }
        #     }]
        # )
        
        # for record in tqdm(results):
        #     table.put_item(
        #         Item={
        #             constants.TABLE_PARTITION_KEY:  "MPAY#1",
        #             constants.TABLE_SORTING_KEY:    "PAY#" + str(record[0]),
        #             "GSI-1-PK"  : "MSTF#1",
        #             "GSI-1-SK"  : "STF#" + str(record[1]),
        #             "amount"    : record[2]})

        # log("Done\n")
        
        # ========== ACCESS PATTERN 5 ==================================
        # log("Querying data from postgres for access pattern 5")
        # cur.execute("""
        #             SELECT customer_id, rental_id 
        #             FROM rental""")
        # results = cur.fetchall()

        # log("Inserting into DynamoDB")

        # for record in tqdm(results):
        #     table.put_item(
        #         Item={
        #             constants.TABLE_PARTITION_KEY:  "MCST#1",
        #             constants.TABLE_SORTING_KEY:    "CST#" + str(record[0]) + 
        #                                             "RNT#" + str(record[1])})
        
        # log("Done\n")


        # ========== ACCESS PATTERN 6 ==================================
        # As mentioned in pattern 4 we can now utilize that we have a 
        # partition for payments. Here we can again create another index
        # only this time for the customer rather than the staff.
        # Similar to pattern 3 we again then use the customer id to get
        # the name and store to avoid data redundancy. 
        # We also have to do the aggregation part again in the client 
        # when we are doing the fetching.

        # log("Querying data from postgres for access pattern 6")
        # cur.execute("""
        #             SELECT payment_id, customer_id, amount
        #             FROM payment """)
        # results = cur.fetchall()

        # log("Inserting into DynamoDB")
        # table.update(
        #     AttributeDefinitions=[
        #         {"AttributeName": "GSI-2-PK", "AttributeType": "S"},
        #         {"AttributeName": "GSI-2-SK", "AttributeType": "S"}],
        #     GlobalSecondaryIndexUpdates=[{ 
        #         "Create": {
        #             "IndexName" : "GSI-2",
        #             "KeySchema" : [
        #                 {"AttributeName" : "GSI-2-PK", "KeyType" : "HASH"},
        #                 {"AttributeName" : "GSI-2-SK", "KeyType" : "RANGE"}],
        #             "Projection" : {
        #                 "ProjectionType" : "ALL"}
        #         }
        #     }]
        # )
        # # This time we update the payments to include our new index
        # for record in tqdm(results):
        #     table.update_item(
        #         Key={
        #             constants.TABLE_PARTITION_KEY:  "MPAY#1",
        #             constants.TABLE_SORTING_KEY:    "PAY#" + str(record[0])},
        #         AttributeUpdates={    
        #             "GSI-2-PK"  : {
        #                 "Value": "MCST#1", 
        #                 "Action": "PUT"},
        #             "GSI-2-SK"  : {
        #                 "Value": "CST#" + str(record[1]), 
        #                 "Action": "PUT"}})
            
        # # Now we create the more detailed information for the customer 
        # # so we can fetch the store and name
        # log("Querying data from postgres for access pattern 5")
        # cur.execute("""
        #             SELECT customer_id, first_name, last_name, store_id 
        #             FROM customer""")
        # results = cur.fetchall()

        # log("Inserting into DynamoDB")
        # for record in tqdm(results):
        #     table.put_item(
        #         Item={
        #             constants.TABLE_PARTITION_KEY:  "MCST#1",
        #             constants.TABLE_SORTING_KEY:    "CST#" + str(record[0]),
        #             "first_name": str(record[1]),
        #             "last_name": str(record[2]),
        #             "store_id": str(record[3])})
            
        # log("Done\n")

        # ========== ACCESS PATTERN 7 ==================================    
        # We decided to assign the rental to the movie, as this allows 
        # us to have a more efficient setup for access pattern 

        # ========== ACCESS PATTERN 8 ==================================
        # ========== ACCESS PATTERN 9 ==================================
        # ========== ACCESS PATTERN 10 =================================
        # log("Querying data from postgres for access pattern 10")
        # cur.execute("""
        #             SELECT staff_id, 
        #             first_name,
        #             last_name,
        #             email,
        #             active,
        #             username,
        #             password,
        #             picture 
        #             FROM staff""")
        # results = cur.fetchall()

        # log("Inserting into DynamoDB")

        # for record in tqdm(results):
        #     table.put_item(
        #         Item={
        #             constants.TABLE_PARTITION_KEY:  "MSTF#1",
        #             constants.TABLE_SORTING_KEY:    "STF#" + str(record[0]),
        #             "first_name":   str(record[1]),
        #             "last_name":    str(record[2]),
        #             "email":        str(record[3]),
        #             "active":       str(record[4]),
        #             "username":     str(record[5]),
        #             "password":     str(record[6]),
        #             "picture":      str(record[7])})
        
        # log("Done\n")

        # ========== ACCESS PATTERN 11 =================================
        # ========== ACCESS PATTERN 12 =================================
        # ========== ACCESS PATTERN 13 =================================
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
        # Dynamo does not allow for aggregation within queries, so we
        # will have to manually create the ranking
        response = table.query(
            KeyConditionExpression=
                boto3.dynamodb.conditions.Key(
                    constants.TABLE_PARTITION_KEY).eq("MACT#1")
        )

        c41 = {}
        for item in response["Items"]:
            # RegEx that splits composite keys
            actor_id = re.split(r"(?<=[0-9])(?=[a-zA-Z])", 
                                item[constants.TABLE_SORTING_KEY])[0]
            if actor_id in c41: 
                c41[actor_id] += 1   
            else:
                c41[actor_id] = 0

        # Sort by value in descending order
        c42 = sorted(c41.items(), key=lambda x: -x[1])


        c4 = []
        # Fetch the first and last name of the top 10 most appearing 
        # actors
        for i in range(10):
            actor_id, _ = c42[i]
            response = table.query(
                KeyConditionExpression=
                    boto3.dynamodb.conditions.Key(
                        constants.TABLE_PARTITION_KEY).eq("MACT#1") &
                    boto3.dynamodb.conditions.Key(
                        constants.TABLE_SORTING_KEY).eq(actor_id))
            
            actor = response["Items"][0]
            c4.append(actor["first_name"] + " " + actor["last_name"])

        # ========== 4.D ===============================================
        response = table.query(
            IndexName="GSI-1",
            KeyConditionExpression=
                boto3.dynamodb.conditions.Key("GSI-1-PK").eq("MSTF#1")
        )

        last_key    = response.get("LastEvaluatedKey")
        items       = response["Items"]    

        # This query would require more items to be returned than dynamo
        # allows, so we need to paginate over the query until all values
        # have been returned
        while last_key:
            response = table.query(
                IndexName="GSI-1",
                KeyConditionExpression=
                    boto3.dynamodb.conditions.Key("GSI-1-PK").eq("MSTF#1"),
                ExclusiveStartKey=last_key
            )
        
            items.extend(response["Items"])
            last_key = response.get("LastEvaluatedKey")

        # Manual aggregation, as Dynamo does not have an option to do
        # this in queries
        d4 = {}
        for item in items:
            if item["GSI-1-SK"] in d4: 
                d4[item["GSI-1-SK"]] += item["amount"]   
            else:
                d4[item["GSI-1-SK"]] = item["amount"] 
        
        # ========== 4.E ===============================================
        response = table.query(
            KeyConditionExpression=
                boto3.dynamodb.conditions.Key(
                    constants.TABLE_PARTITION_KEY).eq("MCST#1"))

        last_key    = response.get("LastEvaluatedKey")
        items       = response["Items"]   

        while last_key:
            response = table.query(
                KeyConditionExpression=
                    boto3.dynamodb.conditions.Key(
                        constants.TABLE_PARTITION_KEY).eq("MCST#1"),
                ExclusiveStartKey=last_key
            )
        
            items.extend(response["Items"])
            last_key = response.get("LastEvaluatedKey")

        e41 = {}
        for item in items:
            # RegEx that splits composite keys
            customer_id = re.split(r"(?<=[0-9])(?=[a-zA-Z])", 
                                item[constants.TABLE_SORTING_KEY])[0]
            if customer_id in e41: 
                e41[customer_id] += 1   
            else:
                e41[customer_id] = 1

        # Sort by value in descending order
        e42 = sorted(e41.items(), key=lambda x: -x[1])
        # Only the IDs
        e4 = [t[0] for t in e42[:10]]

        # ========== 4.F ===============================================
        response = table.query(
            IndexName="GSI-2",
            KeyConditionExpression=
                boto3.dynamodb.conditions.Key("GSI-2-PK").eq("MCST#1")
        )

        last_key    = response.get("LastEvaluatedKey")
        items       = response["Items"]    

        # This query would require more items to be returned than dynamo
        # allows, so we need to paginate over the query until all values
        # have been returned
        while last_key:
            response = table.query(
                IndexName="GSI-2",
                KeyConditionExpression=
                    boto3.dynamodb.conditions.Key("GSI-2-PK").eq("MCST#1"),
                ExclusiveStartKey=last_key
            )
        
            items.extend(response["Items"])
            last_key = response.get("LastEvaluatedKey")

        # Manual aggregation, as Dynamo does not have an option to do
        # this in queries
        f41 = {}
        for item in items:
            if item["GSI-2-SK"] in f41: 
                f41[item["GSI-2-SK"]] += item["amount"]   
            else:
                f41[item["GSI-2-SK"]] = item["amount"] 

        f42 = sorted(f41.items(), key=lambda x: -x[1])

        f4 = []
        # Fetch the first and last name of the top 10 most appearing 
        # actors
        for i in range(10):
            customer_id, _ = f42[i]
            response = table.query(
                KeyConditionExpression=
                    boto3.dynamodb.conditions.Key(
                        constants.TABLE_PARTITION_KEY).eq("MCST#1") &
                    boto3.dynamodb.conditions.Key(
                        constants.TABLE_SORTING_KEY).eq(customer_id))
            
            customer = response["Items"][0]
            f4.append(customer["first_name"] + " " + 
                      customer["last_name"] + ", " + 
                      customer["store_id"] )

        # ========== 4.G ===============================================
        # ========== 4.H ===============================================
        # ========== 4.I ===============================================


        print("========== UPDATE ============================================")
        # ========== 5.A ===============================================
        response = table.query(
            KeyConditionExpression=
                boto3.dynamodb.conditions.Key(   
                    constants.TABLE_PARTITION_KEY).eq("MSTF#1")
        )

        for item in response["Items"]:
            table.update_item(
                Key={
                    constants.TABLE_PARTITION_KEY:  
                        item[constants.TABLE_PARTITION_KEY],
                    constants.TABLE_SORTING_KEY: 
                        item[constants.TABLE_SORTING_KEY]},
                AttributeUpdates={    
                    "password"  : {
                        "Value": str(uuid.uuid4()), 
                        "Action": "PUT"}})

        # Query only for the tests
        response = table.query(
            KeyConditionExpression=
                boto3.dynamodb.conditions.Key(   
                    constants.TABLE_PARTITION_KEY).eq("MSTF#1")
        ) 
        a5 = []   
        for item in response["Items"]:    
            a5.append(item["password"])

        # ========== 5.B ===============================================

        print("========== DELETE ============================================")
        # ========== 6.A ===============================================
        # ========== 6.B ===============================================




        # ==============================================================
        conn.commit()


        # ========== TESTS =============================================
        def test(expected, result, desc, contains=False, equality=True):
            passed = expected == result if equality else expected != result
            exp_ext = "" if equality else "NOT "
            c = GREEN if passed else RED
            ext = "✅ PASSED! " if passed else "❌ FAILED! "

            print("TESTING: "   + BLUE + desc + CLEAR)
            print("EXPECTED: "  + GREEN + exp_ext + str(expected) + CLEAR) 
            print("GOT: "       + c + str(result))
            print(ext + CLEAR)
            print()

        top10_ac = ["Gina Degeneres", 
                    "Walter Torn", 
                    "Mary Keitel", 
                    "Matthew Carrey", 
                    "Sandra Kilmer", 
                    "Scarlett Damon", 
                    "Groucho Dunst", 
                    "Uma Wood", 
                    "Angela Witherspoon", 
                    "Vivien Basinger"]
        
        top10_cust_rental = ["CST#148",
                             "CST#526",
                             "CST#144",
                             "CST#236",
                             "CST#75",
                             "CST#197",
                             "CST#469",
                             "CST#137",
                             "CST#178",
                             "CST#468"]
        
        top10_cust_pay = []

        prev_pwds = ["8cb2237d0679ca88db6464eac60da96345513964",
                     "8cb2237d0679ca88db6464eac60da96345513964"]


        print("========== TEST ==============================================")
        # 4
        test(4581, a4, 
             "Gesamtanzahl der verfügbaren Filme")
        test((759, 762), b4, 
             "Anzahl der Unterschiedlichen Filme je Standort")
        test(top10_ac, c4, 
             """Die Vor- und Nachnamen der 10 Schauspieler mit den meisten 
             Filmen, absteigend sortiert""")
        test({"STF#1": Decimal("30252.12"), "STF#2": Decimal("31059.92")}, d4, 
             """Die Erlöse je Mitarbeiter """)
        test(top10_cust_rental, e4, 
             """Die IDs der 10 Kunden mit den meisten Entleihungen """)
        test(top10_cust_pay, f4, 
             """Die Vor- und Nachnamen sowie die Niederlassung der 10 Kunden, 
             die das meiste Geld ausgegeben haben """)

        #5
        test(prev_pwds, a5, 
             """Vergebt allen Mitarbeitern ein neues, sicheres Passwort """, 
             equality=False)

        #6

 