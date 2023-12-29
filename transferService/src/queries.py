import  test
import  re
import  uuid

from    constants                   import *
from    logger                      import log
from    boto3.dynamodb.conditions   import Attr, Key

# Main function to run all queries
def run_queries(conn):
    log("========== READ ==============================================")
    # ========== 4.A ===============================================
    log("QUERY 4.A: Total number of films available")

    response = TABLE.query(
        KeyConditionExpression=Key(
            TABLE_PARTITION_KEY).eq("MINV#1"),
        Select="COUNT"
    )
    a4 = response["Count"]

    log("Result:")
    log(a4)
    log("Done\n")

    # ========== 4.B ===============================================
    log("QUERY 4.B: Number of films per store")

    response = TABLE.query(
        KeyConditionExpression=Key(
            TABLE_PARTITION_KEY).eq("STR#1"),
        Select="COUNT"
    )
    b41 = response["Count"]

    response = TABLE.query(
        KeyConditionExpression=Key(
            TABLE_PARTITION_KEY).eq("STR#2"),
        Select="COUNT"
    )
    b42 = response["Count"]

    b4 = (b41, b42)

    log("Result:")
    log(b4)
    log("Done\n")

    # ========== 4.C ===============================================
    log("QUERY 4.C: Names of the 10 actors with most films")

    # Dynamo does not allow for aggregation within queries, so we
    # will have to manually create the ranking
    response = TABLE.query(
        KeyConditionExpression=
            Key(TABLE_PARTITION_KEY).eq("MACT#1")
    )

    c41 = {}
    for item in response["Items"]:
        actor_key = 'ACT#' + item[TABLE_SORTING_KEY].split('ACT#')[1]
        if actor_key in c41: 
            c41[actor_key] += 1
        else:
            c41[actor_key] = 0

    # Sort by value in descending order
    c42 = sorted(c41.items(), key=lambda x: -x[1])

    c4 = []
    # Fetch the first and last name of the top 10 most appearing 
    # actors
    for i in range(10):
        actor_key, _ = c42[i]
        response = TABLE.query(
            KeyConditionExpression=
                Key(
                    TABLE_PARTITION_KEY).eq('MACT#2') &
                Key(
                    TABLE_SORTING_KEY).eq(actor_key))
        
        actor = response["Items"][0]
        c4.append(actor["first_name"] + " " + actor["last_name"])

    log("Result:")
    log(c4)
    log("Done\n")

    # ========== 4.D ===============================================
    log("QUERY 4.D: Revenue per employee")
    response = TABLE.query(
        IndexName="GSI-1",
        KeyConditionExpression=
            Key("GSI-1-PK").eq("MSTF#1")
    )

    last_key    = response.get("LastEvaluatedKey")
    items       = response["Items"]    

    # This query would require more items to be returned than dynamo
    # allows, so we need to paginate over the query until all values
    # have been returned
    while last_key:
        response = TABLE.query(
            IndexName="GSI-1",
            KeyConditionExpression=
                Key("GSI-1-PK").eq("MSTF#1"),
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

    log("Result:")
    log(d4)
    log("Done\n")

    # ========== 4.E ===============================================
    log("QUERY 4.E: 10 customers with the most rentals")
    response = TABLE.query(
        KeyConditionExpression=
            Key(
                TABLE_PARTITION_KEY).eq("MCST#1"))

    last_key    = response.get("LastEvaluatedKey")
    items       = response["Items"]   

    while last_key:
        response = TABLE.query(
            KeyConditionExpression=
                Key(
                    TABLE_PARTITION_KEY).eq("MCST#1"),
            ExclusiveStartKey=last_key
        )

        items.extend(response["Items"])
        last_key = response.get("LastEvaluatedKey")

    e41 = {}
    for item in items:
        # RegEx that splits composite keys
        customer_id = re.split(r"(?<=[0-9])(?=[a-zA-Z])", 
                            item[TABLE_SORTING_KEY])[0]
        if customer_id in e41: 
            e41[customer_id] += 1   
        else:
            e41[customer_id] = 1

    # Sort by value in descending order
    e42 = sorted(e41.items(), key=lambda x: -x[1])
    # Only the IDs
    e4 = [t[0] for t in e42[:10]]

    log("Result:")
    log(e4)
    log("Done\n")

    # ========== 4.F ===============================================
    log("QUERY 4.F: Names and store of the 10 customers who spent "
        "the most")
    
    response = TABLE.query(
        IndexName="GSI-2",
        KeyConditionExpression=
            Key("GSI-2-PK").eq("MCST#1")
    )

    last_key    = response.get("LastEvaluatedKey")
    items       = response["Items"]    

    # This query would require more items to be returned than dynamo
    # allows, so we need to paginate over the query until all values
    # have been returned
    while last_key:
        response = TABLE.query(
            IndexName="GSI-2",
            KeyConditionExpression=
                Key("GSI-2-PK").eq("MCST#1"),
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
        response = TABLE.query(
            KeyConditionExpression=
                Key(
                    TABLE_PARTITION_KEY).eq("MCST#1") &
                Key(
                    TABLE_SORTING_KEY).eq(customer_id))
        
        customer = response["Items"][0]
        f4.append(customer["first_name"] + " " + 
                    customer["last_name"] + ", " + 
                    customer["store_id"] )

    log("Result:")
    log(f4)
    log("Done\n")

    # ========== 4.G ===============================================
    log("QUERY 4.G: 10 most rented films")

    # Get all STR# FLM#INV# items
    filter_expression = Attr(TABLE_PARTITION_KEY).begins_with("STR#") & \
                        Attr(TABLE_SORTING_KEY).begins_with("FLM#")
    response = TABLE.scan(FilterExpression=filter_expression)
    items = response["Items"]

    # Sum up rental counts for equal STR#FLM# keys
    rental_count_sums = {}
    for item in items:
        pk_value = item.get(TABLE_PARTITION_KEY)
        sk_value = item.get(TABLE_SORTING_KEY)
        rental_count = item.get('rental_count', 0)

        # Remove INV# part of SK
        film_sk = sk_value.split("INV#")[0]
        key = f"{pk_value}_{film_sk}"

        rental_count_sums[key] = rental_count_sums.get(key, 0) + rental_count

    # Get titles for db
    film_data = []
    for key, rental_count in rental_count_sums.items():
        # Extract the FLM# part of the key
        film_key = key.split("_")[1]

        # Query for the film title
        response = TABLE.query(
            KeyConditionExpression= \
                Key(TABLE_PARTITION_KEY).eq(film_key) & \
                Key(TABLE_SORTING_KEY).eq("FLM")
        )

        # Error handling in case film has been deleted
        try:
            film_data.append((response["Items"][0]["title"], int(rental_count)))
        except Exception as e:
            if not response["Items"]:
                log(f"Error: Film with partition key {film_key} not found")
            else:
                log(f"Error: {e}")

    # Sort, descending order
    sorted_film_data = sorted(film_data, key=lambda x: x[1], reverse=True)
    top_10_films = []
    for i in range(10):
        top_10_films.append({
                "name": sorted_film_data[i][0],
                "rental_count": sorted_film_data[i][1]
                })

    log("Result:")
    log(top_10_films)
    log("Done\n")

    # ========== 4.H ===============================================
    log("QUERY 4.H: 3 most rented categories")

    response = TABLE.query(
        KeyConditionExpression= \
            Key(TABLE_PARTITION_KEY).eq('MCTGR#1'),
        ScanIndexForward=False,  # Descending order
        Limit=3
    )
    items = response["Items"]

    category_list = [{
            "name": item.get("name"),
            "rental_count": int(item.get(TABLE_SORTING_KEY).replace("RNTLCNT#", ""))}
        for item in items]
    top_3_categories = sorted(category_list, key=lambda x: x["rental_count"], reverse=True)

    log("Result:")
    log(top_3_categories)
    log("Done\n")

    # ========== 4.I ===============================================
    log("QUERY 4.I: Customer overview list")

    response = TABLE.query(
        KeyConditionExpression= \
            Key(TABLE_PARTITION_KEY).eq('MCST#2')
    )
    items = response["Items"]

    # Create customer list
    customers = []
    for item in items:
        customer = {
            'customer_id': int(item.get(TABLE_SORTING_KEY).split('#')[1]),
            'name': item.get("name"),
            'address': item.get("address"),
            'zip_code': item.get("zip_code"),
            'phone': item.get("phone"),
            'city': item.get("city"),
            'country': item.get("country"),
            'notes': item.get("notes"),
            'sid': item.get("sid")
        }
        customers.append(customer)

    # Sort ascending
    customers_sorted = sorted(customers, key=lambda x: int(x['customer_id']))
    
    log("Result:")
    for customer in customers_sorted[:10]:
        log(customer)
    
    log("Done\n")

    log("========== UPDATE ============================================")
    # ========== 5.A ===============================================
    log("QUERY 5.A: New staff passwords")

    response = TABLE.query(
        KeyConditionExpression=
            Key(   
                TABLE_PARTITION_KEY).eq("MSTF#1")
    )

    for item in response["Items"]:
        TABLE.update_item(
            Key={
                TABLE_PARTITION_KEY:  
                    item[TABLE_PARTITION_KEY],
                TABLE_SORTING_KEY: 
                    item[TABLE_SORTING_KEY]},
            AttributeUpdates={    
                "password"  : {
                    "Value": str(uuid.uuid4()),
                    "Action": "PUT"}})

    # Query only for the tests
    response = TABLE.query(
        KeyConditionExpression=
            Key(   
                TABLE_PARTITION_KEY).eq("MSTF#1")
    ) 
    a5 = []   
    for item in response["Items"]:    
        a5.append(item["password"])

    log("Result:")
    log(a5)
    log("Done\n")

    # ========== 5.B ===============================================
    log("QUERY 5.B: New store with all inventories")

    log("Inserting into DynamoDB")
    TABLE.put_item(
        Item={
            TABLE_PARTITION_KEY: "MSTR#1",
            TABLE_SORTING_KEY: "STR#3",
            'address_id' : str(100000),
            'address_address' : 'Feldstrasse 143',
            'address_address2' : '',
            'address_district' : 'Wedel',
            'address_city' : 'Wedel',
            'address_country' : 'Germany',
            'address_postal_code' : '22880',
            'address_phone' : '+49410380480'
        })
    
    # Get all inventory items
    filter_expression = Attr(TABLE_PARTITION_KEY).begins_with("STR#")
    response = TABLE.scan(FilterExpression=filter_expression)

    # Update stores
    log("Inserting into DynamoDB")
    for item in response['Items']:
        # Can't directly update partition keys, need to delete and re-add
        TABLE.delete_item(
        Key={
                TABLE_PARTITION_KEY: item[TABLE_PARTITION_KEY],
                TABLE_SORTING_KEY: item[TABLE_SORTING_KEY]
            }
        )
        TABLE.put_item(
            Item={
                TABLE_PARTITION_KEY: 'STR#3', 
                TABLE_SORTING_KEY: item[TABLE_SORTING_KEY],
            }
        )
    
    # Get updated valus to log results
    response = TABLE.query(
        KeyConditionExpression=
            Key(TABLE_PARTITION_KEY).eq("MSTR#1"))
    stores = response['Items']

    response = TABLE.query(
        KeyConditionExpression=
            Key(TABLE_PARTITION_KEY).eq('STR#3') & \
            Key(TABLE_SORTING_KEY).begins_with('FLM#'),
        Limit = 10)
    inventory = response['Items']

    log("Result:")
    log("Stores")
    for store in stores:
        log(store)

    log('\n')
    log("Inventory (only displaying the first 10 entries here)")
    log(inventory[:10])
    log("Done\n")

    log("========== DELETE ============================================")
    # ========== 6.A ===============================================
    log("QUERY 6.A: Delete films shorter than 60 minutes")

    # Get all films with length < 60
    log("Fetching films with length < 60")
    condition_expression = Attr(TABLE_PARTITION_KEY).begins_with("FLM#") & \
                            Attr(TABLE_SORTING_KEY).eq("FLM") & \
                            Attr('length').lt(60)
    response = TABLE.scan(FilterExpression=condition_expression)
    to_delete = response['Items']

    log("Deleting from FLM# FLM access pattern (AP 7)")
    for item in to_delete:
        TABLE.delete_item(Key={
                TABLE_PARTITION_KEY: item[TABLE_PARTITION_KEY],
                TABLE_SORTING_KEY: item[TABLE_SORTING_KEY]
            })
        # item[TABLE_PARTITION_KEY] = FLM#XXX here

    log("Deleting from MINV#1 FLM#INV# access pattern (AP1)")
    ap2_pks = {}
    for item in to_delete:
        response_inv = TABLE.query(
            KeyConditionExpression =
                Key(TABLE_PARTITION_KEY).eq('MINV#1') & \
                Key(TABLE_SORTING_KEY).begins_with(item[TABLE_PARTITION_KEY])
        )

        for item_inv in response_inv['Items']:

            # Store FLM# -> STR# mapping to get STR# as PK in next query
            ap2_pks[item[TABLE_PARTITION_KEY]] = 'STR#' + str(item_inv['store_id'])
            
            # Delete item
            TABLE.delete_item(
                Key={
                    TABLE_PARTITION_KEY: item_inv[TABLE_PARTITION_KEY],
                    TABLE_SORTING_KEY: item_inv[TABLE_SORTING_KEY]
                }
            )

    log("Deleting from STR# FLM#INV# access pattern (AP2)")
    for item in to_delete:
        
        # Only try to query if store_id was found (film may not be in inventory)
        if item[TABLE_PARTITION_KEY] in ap2_pks:
            response_strflm = TABLE.query(
                KeyConditionExpression =
                    Key(TABLE_PARTITION_KEY) \
                    .eq(ap2_pks[item[TABLE_PARTITION_KEY]]) & \
                    Key(TABLE_SORTING_KEY).begins_with(item[TABLE_PARTITION_KEY])
            )

            for item_strflm in response_strflm['Items']:
                TABLE.delete_item(
                    Key={
                        TABLE_PARTITION_KEY: item_strflm[TABLE_PARTITION_KEY],
                        TABLE_SORTING_KEY: item_strflm[TABLE_SORTING_KEY]
                    }
                )

    log("Deleting from MACT#1 ACT#FLM# access pattern (AP3)")
    for item in to_delete:
        response_act = TABLE.query(
            KeyConditionExpression =
                Key(TABLE_PARTITION_KEY).eq('MACT#1') & \
                Key(TABLE_SORTING_KEY).begins_with(item[TABLE_PARTITION_KEY])
        )

        for item in response_act["Items"]:
            TABLE.delete_item(
                Key={
                    TABLE_PARTITION_KEY: item[TABLE_PARTITION_KEY],
                    TABLE_SORTING_KEY: item[TABLE_SORTING_KEY]
                }
            )

    # Output results
    lengths = []
    for film in to_delete:
        lengths.append((film['title'], int(film['length'])))

    sorted_lengths = sorted(lengths, key=lambda x: x[1])

    log("Result:")
    log("Names of deleted films and their length (only displaying first 10 here)")
    log(sorted_lengths[:10])
    log("Done\n")

    # ========== 6.B ===============================================
    log("QUERY 6.B: Delete related rentals")

    # Count rentals before deletion
    condition_expression = Attr(TABLE_PARTITION_KEY).begins_with('FLM#') & \
                            Attr(TABLE_SORTING_KEY).eq('RNTL')
    response_rtnl = TABLE.scan(FilterExpression=condition_expression)
    rental_count_before = response_rtnl['Count']

    # Delete rentals
    log("Deleting from FLM# RNTL access pattern (AP13)")
    for item in to_delete:
        TABLE.delete_item(
            Key={
                TABLE_PARTITION_KEY: item[TABLE_PARTITION_KEY],
                TABLE_SORTING_KEY: 'RNTL'
            }
        )

    # Count rentals after deletion
    response_rtnl = TABLE.scan(FilterExpression=condition_expression)
    rental_count_after = response_rtnl['Count']

    log("Result:")
    log(f"Number of rentals before: {rental_count_before}")
    log(f"Number of rentals after:  {rental_count_after}")
    log("Done\n")

    # ==============================================================
    conn.commit()

    # ========== TESTS =============================================
    if RUN_TESTS:
        test.run_tests(a4, b4, c4, d4, e4, f4, a5)