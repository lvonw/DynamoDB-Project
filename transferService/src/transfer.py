import  psycopg
import  time

from    constants                   import *
from    logger                      import log


# Main function for transering data from postgres db to DynamoDB
def run_transfer(conn, cur):

    log("========== CREATE ============================================")
    
    # ========== ACCESS PATTERN 1 ==================================
    log("ACCESS PATTERN 1, MINV#1 FLM#INV#: Total number of films available")
    # As we use a single table approach, it would be inefficient to
    # scan through each partition key, to see which rows are actual
    # inventory items. Therefore we employ a master partition which
    # allows us to query all movies with a single partition key.

    # The film_id is added to the SK so entries can be efficiently
    # deleted later (if length < 60) 

    log("Querying from postgres")
    cur.execute("SELECT film_id, inventory_id FROM inventory")
    results = cur.fetchall()

    log("Inserting into DynamoDB")
    with TABLE.batch_writer() as batch:
        for record in results:
            batch.put_item(
                Item={
                    TABLE_PARTITION_KEY:  "MINV#1",
                    TABLE_SORTING_KEY:    "FLM#" + str(record[0]) + \
                                            "INV#" + str(record[1])})
    log("Done\n")

    # ========== ACCESS PATTERN 2 ==================================
    log("ACCESS PATTERN 2, STR# FLM#INV#: Number of films per store")

    log("Querying from postgres")
    cur.execute("SELECT store_id, film_id, inventory_id FROM inventory")
    results = cur.fetchall()

    log("Inserting into DynamoDB")
    with TABLE.batch_writer() as batch:
        for record in results:
            batch.put_item(
                Item={
                    TABLE_PARTITION_KEY:  "STR#" + str(record[0]),
                    TABLE_SORTING_KEY:    "FLM#" + str(record[1])
                                            + "INV#" + str(record[2])})
    # Added inventory_id to make entries unique and allow for batch writing    
    log("Done\n")

    # ========== ACCESS PATTERN 3 ==================================
    log("ACCESS PATTERN 3, MACT#1 ACT#FLM#: Names of the 10 actors with most films")
    # We again use a master partition to be able to query all actors
    # without looking through all partition keys. We use composite
    # sorting keys to associate a movie with an actor.
    # To avoid data redundancy we then create another entry under 
    # the master actor partition. This time only with the actor id 
    # as the sorting key in order to recieve the first and last name

    log("Querying from postgres")
    cur.execute("""
                SELECT film_id, fa.actor_id, first_name, last_name 
                FROM film_actor AS fa
                INNER JOIN actor AS a
                ON fa.actor_id = a.actor_id""")
    results = cur.fetchall()

    log("Inserting into DynamoDB")
    # Can't use batch writing here, as items contain duplicate keys
    for record in results:
        TABLE.put_item(
            Item={
                TABLE_PARTITION_KEY:  "MACT#1",
                TABLE_SORTING_KEY:    "FLM#" + str(record[0]) +
                                        "ACT#" + str(record[1])})
        TABLE.put_item(
            Item={
                TABLE_PARTITION_KEY:  "MACT#2",
                TABLE_SORTING_KEY:    "ACT#" + str(record[1]),
                "first_name": str(record[2]),
                "last_name": str(record[3])})
    log("Done\n")

    # ========== ACCESS PATTERN 4 ==================================
    log("ACCESS PATTERN 4, MPAY#1-MSTF#1-GSI: Revenue per employee")
    # Associate staff with payments via secondary index. We do it in
    # this fashion, because this allows us to do the same with the 
    # customers in Access Pattern 7. 

    log("Querying from postgres")
    cur.execute("""
                SELECT payment_id, staff_id, amount
                FROM payment """)
    results = cur.fetchall()

    log("Inserting into DynamoDB")
    TABLE.update(
        AttributeDefinitions=[
            {"AttributeName": "GSI-1-PK", "AttributeType": "S"},
            {"AttributeName": "GSI-1-SK", "AttributeType": "S"},
            {"AttributeName": "amount",   "AttributeType": "N"}],
        GlobalSecondaryIndexUpdates=[{ 
            "Create": {
                "IndexName" : "GSI-1",
                "KeySchema" : [
                    {"AttributeName" : "GSI-1-PK", "KeyType" : "HASH"},
                    {"AttributeName" : "GSI-1-SK", "KeyType" : "RANGE"}],
                "Projection" : {
                    "ProjectionType" : "ALL"}
            }
        }]
    )

    for record in results:
        TABLE.put_item(
            Item={
                TABLE_PARTITION_KEY:  "MPAY#1",
                TABLE_SORTING_KEY:    "PAY#" + str(record[0]),
                "GSI-1-PK"  : "MSTF#1",
                "GSI-1-SK"  : "STF#" + str(record[1]),
                "amount"    : record[2]})
    log("Done\n")

    # ========== ACCESS PATTERN 5 ==================================
    log("ACCESS PATTERN 5, MCST#1 CST#RNT#: 10 customers with the most rentals")

    log("Querying from postgres")
    cur.execute("""
                SELECT customer_id, rental_id 
                FROM rental""")
    results = cur.fetchall()

    log("Inserting into DynamoDB")
    with TABLE.batch_writer() as batch:
        for record in results:
            batch.put_item(
                Item={
                    TABLE_PARTITION_KEY:  "MCST#1",
                    TABLE_SORTING_KEY:    "CST#" + str(record[0]) + 
                                                    "RNT#" + str(record[1])})
    log("Done\n")

    # ========== ACCESS PATTERN 6 ==================================
    log("ACCESS PATTERN 6, MPAY#1-MCST#1-GSI: Names and store of the "
        "10 customers who spent the most")
    # As mentioned in pattern 4 we can now utilize that we have a 
    # partition for payments. Here we can again create another index
    # only this time for the customer rather than the staff.
    # Similar to pattern 3 we again then use the customer id to get
    # the name and store to avoid data redundancy. 
    # We also have to do the aggregation part again in the client 
    # when we are doing the fetching.

    log("Querying from postgres")
    cur.execute("""
                SELECT payment_id, customer_id, amount
                FROM payment """)
    results = cur.fetchall()

    log("Inserting into DynamoDB")
    with TABLE.batch_writer() as batch:
        # This should remain table.update(), not batch.update()
        TABLE.update(
            AttributeDefinitions=[
                {"AttributeName": "GSI-2-PK", "AttributeType": "S"},
                {"AttributeName": "GSI-2-SK", "AttributeType": "S"}],
            GlobalSecondaryIndexUpdates=[{ 
                "Create": {
                    "IndexName" : "GSI-2",
                    "KeySchema" : [
                        {"AttributeName" : "GSI-2-PK", "KeyType" : "HASH"},
                        {"AttributeName" : "GSI-2-SK", "KeyType" : "RANGE"}],
                    "Projection" : {
                        "ProjectionType" : "ALL"}
                }
            }]
        )
        
        # This time we update the payments to include our new index
        # Use put_item() instead because batch writer doesn't support update()
        log("Inserting into DynamoDB")
        for record in results:
            batch.put_item(
                Item={
                    TABLE_PARTITION_KEY:  "MPAY#1",
                    TABLE_SORTING_KEY:    "PAY#" + str(record[0]),
                    "GSI-2-PK": "MCST#1",
                    "GSI-2-SK": "CST#" + str(record[1]),
                    "amount": record[2]})

        
        # Now we create the more detailed information for the customer 
        # so we can fetch the store and name
        log("Querying data from postgres for access pattern 6")
        cur.execute("""
                    SELECT customer_id, first_name, last_name, store_id 
                    FROM customer""")
        results = cur.fetchall()

        log("Inserting into DynamoDB")
        for record in results:
            batch.put_item(
                Item={
                    TABLE_PARTITION_KEY:  "MCST#1",
                    TABLE_SORTING_KEY:    "CST#" + str(record[0]),
                    "first_name": str(record[1]),
                    "last_name": str(record[2]),
                    "store_id": str(record[3])})
    log("Done\n")

    # ========== ACCESS PATTERN 7 ==================================
    log("ACCESS PATTERN 7, FLM# FLM: 10 most rented films")
    # We decided to assign the rental counts to the inventory items, as
    # this means we don't have to store the rentals for this application,
    # which there are a lot of. So we save a good amount of storage.
    
    log("Querying from postgres")
    cur.execute("""
        SELECT inventory.store_id,
                inventory.film_id,
                rental.inventory_id,
                COUNT(rental.rental_id)
        FROM rental
        JOIN inventory ON rental.inventory_id = inventory.inventory_id
        GROUP BY rental.inventory_id, inventory.film_id, inventory.store_id
    """)
    results = cur.fetchall()
    
    # Add rental_count to existing access pattern (AP2)
    log("Inserting into DynamoDB")
    with TABLE.batch_writer() as batch:
        for record in results:
            batch.put_item(
                Item={
                    TABLE_PARTITION_KEY: "STR#" + str(record[0]),
                    TABLE_SORTING_KEY: "FLM#" + str(record[1]) \
                                        + "INV#" + str(record[2]),
                    "rental_count": int(record[3])
                })
            
    log("Querying from postgres")
    cur.execute("""
        SELECT film_id, title
        FROM film
    """)
    results = cur.fetchall()

    # Store films for looking up titles
    log("Inserting into DynamoDB")
    with TABLE.batch_writer() as batch:
        for record in results:
            batch.put_item(
                Item={
                    TABLE_PARTITION_KEY: 'FLM#' + str(record[0]),
                    TABLE_SORTING_KEY: 'FLM',
                    'title': str(record[1])
                })
    log("Done\n")

    # ========== ACCESS PATTERN 8 ==================================
    log("ACCESS PATTERN 8, MCTGR#1 RNTLCNT#: 3 most rented categories")
    # As before, we store the category counts in the table. This time
    # it makes sense, as this is our only use for categories in this
    # application. By storing the counts as sort keys, queries become
    # very efficient.

    log("Querying from postgres")
    cur.execute("""
        SELECT COUNT(rental.rental_id), category.name
        FROM rental
        JOIN inventory ON rental.inventory_id = inventory.inventory_id
        JOIN film_category ON inventory.film_id = film_category.film_id
        JOIN category ON film_category.category_id = category.category_id
        GROUP BY category.category_id, category.name
    """)
    results = cur.fetchall()

    log("Inserting into DynamoDB")
    # Can't use batch calls here because of duplicate keys
    for record in results:
        TABLE.put_item(
            Item={
                TABLE_PARTITION_KEY: "MCTGR#1",
                TABLE_SORTING_KEY: "RNTLCNT#" + str(record[0]),
                'name': str(record[1])
            })
    log("Done\n")

    # ========== ACCESS PATTERN 9 ==================================
    log("ACCESS PATTERN 9, MCST#2 CST#: Customer overview list")
    # As we store a lot of attributes with this access pattern, it
    # makes sense to create a new access pattern and not added to
    # the existing pattern for customer, which already has rental
    # as part of the sort key.

    log("Querying from postgres")
    cur.execute("""
    SELECT customer.customer_id,
                customer.first_name,
                customer.last_name,
                address.address,
                address.postal_code,
                address.phone,
                city.city,
                country.country,
                customer.active,
                customer.store_id
        FROM customer
        JOIN address ON customer.address_id = address.address_id
        JOIN city ON address.city_id = city.city_id
        JOIN country ON city.country_id = country.country_id
    """)
    results = cur.fetchall()

    log("Inserting into DynamoDB")
    with TABLE.batch_writer() as batch:
        for record in results:
            batch.put_item(
                Item={
                    TABLE_PARTITION_KEY: "MCST#2",
                    TABLE_SORTING_KEY: 'CST#' + str(record[0]),
                    'name':         str(record[1]) + " " +  str(record[2]),
                    'address':      str(record[3]),
                    'zip_code':     str(record[4]),
                    'phone':        str(record[5]),
                    'city':         str(record[6]),
                    'country':      str(record[7]),
                    'notes':        str(record[8]),
                    'sid':          str(record[9]),
                })
    log("Done\n")

    # ========== ACCESS PATTERN 10 =================================
    log("ACCESS PATTERN 10, MSTF#1 STF#: New staff passwords")

    log("Querying from postgres")
    cur.execute("""
                SELECT staff_id, 
                first_name,
                last_name,
                email,
                active,
                username,
                password,
                picture 
                FROM staff""")
    results = cur.fetchall()

    log("Inserting into DynamoDB")
    with TABLE.batch_writer() as batch:
        for record in results:
            batch.put_item(
                Item={
                    TABLE_PARTITION_KEY:  "MSTF#1",
                    TABLE_SORTING_KEY:    "STF#" + str(record[0]),
                    "first_name":   str(record[1]),
                    "last_name":    str(record[2]),
                    "email":        str(record[3]),
                    "active":       str(record[4]),
                    "username":     str(record[5]),
                    "password":     str(record[6]),
                    "picture":      str(record[7])})
    log("Done\n")

    # ========== ACCESS PATTERN 11 =================================
    log("ACCESS PATTERN 11, MSTR#1 STR#: New store with all inventories")

    log("Querying from postgres")
    cur.execute("""
        SELECT store.store_id,
                staff.staff_id,
                staff.first_name,
                staff.last_name,
                staff.address_id,
                staff.email,
                staff.active,
                staff.username,
                staff.password,
                staff.picture,
                address.address_id,
                address.address,
                address.address2,
                address.district,
                city.city,
                country.country,
                address.postal_code,
                address.phone
        FROM store
        JOIN staff ON store.manager_staff_id = staff.staff_id
        JOIN address ON store.address_id = address.address_id
        JOIN city ON address.city_id = city.city_id
        JOIN country ON city.country_id = country.country_id
    """)
    results = cur.fetchall()

    log("Inserting into DynamoDB")
    with TABLE.batch_writer() as batch:
        for record in results:
            batch.put_item(
                Item={
                    TABLE_PARTITION_KEY: "MSTR#1",
                    TABLE_SORTING_KEY: "STR#" + str(record[0]),
                    'manager_staff_id' : str(record[1]),
                    'staff_first_name' : str(record[2]),
                    'staff_last_name' : str(record[3]),
                    'staff_address_id' : str(record[4]),
                    'staff_email' : str(record[5]),
                    'staff_active' : str(record[6]),
                    'staff_username' : str(record[7]),
                    'staff_password' : str(record[8]),
                    'staff_picture' : str(record[9]),
                    'address_id' : str(record[10]),
                    'address_address' : str(record[11]),
                    'address_address2' : str(record[12]),
                    'address_district' : str(record[13]),
                    'address_city' : str(record[14]),
                    'address_country' : str(record[15]),
                    'address_postal_code' : str(record[16]),
                    'address_phone' : str(record[17])
                })
    log("Done\n")

    # ========== ACCESS PATTERN 12 =================================
    log("ACCESS PATTERN 12, FLM# FLM: Delete films shorter than 60 minutes")
    # We add the film length to already existing pattern for films.
    # We also add the store_id to the access pattern for inventory
    # entries (AP1), as this allows us to retrieve the store_id of
    # films using the film_id, which is part of the sort key in the
    # pattern. This allows us to delete entries from the store-film
    # access pattern (AP2) efficiently, because that pattern uses the
    # store_id as its partition key.

    log("Querying from postgres")
    cur.execute("""
        SELECT film_id, title, length
        FROM film
    """)
    results = cur.fetchall()

    # Add length to items (using put_item so batch writing is possible)
    log("Inserting into DynamoDB")
    with TABLE.batch_writer() as batch:
        for record in results:
            batch.put_item(
                Item = {
                    TABLE_PARTITION_KEY: 'FLM#' + str(record[0]),
                    TABLE_SORTING_KEY: 'FLM',
                    'title': str(record[1]),
                    'length': int(record[2])
                }
            )

    # Add film_id to access pattern 1 to allow deleteion based on it
    log("Querying from postgres")
    cur.execute("""
        SELECT film_id, inventory_id, store_id
        FROM inventory
    """)
    results = cur.fetchall()

    log("Inserting into DynamoDB")
    with TABLE.batch_writer() as batch:
        for record in results:
            batch.put_item(
                Item = {
                    TABLE_PARTITION_KEY: 'MINV#1',
                    TABLE_SORTING_KEY:    "FLM#" + str(record[0]) + \
                                            "INV#" + str(record[1]),
                    'store_id': int(record[2])
                    # Add store_id for deleting from STR# FLM#INV# pattern
                }
            )
    log("Done\n")

    # ========== ACCESS PATTERN 13 =================================
    log("ACCESS PATTERN 13, FLM# RNTL: Delete related rentals")
    # We add a new access patern with rather minimal attributes for
    # the rental here, which allows efficient deletion using the
    # film key by using it as the partition key.
            
    log("Querying from postgres")
    cur.execute("""
        SELECT inventory.film_id,
                rental_id,
                rental_date,
                rental.inventory_id,
                customer_id,
                return_date,
                staff_id
        FROM rental
        JOIN inventory ON rental.inventory_id = inventory.inventory_id
    """)
    results = cur.fetchall()

    log("Inserting into DynamoDB")
    for record in results:
        #Can't use batch calls here because of duplicate keys
        TABLE.put_item(
            Item = {
                TABLE_PARTITION_KEY: 'FLM#' + str(record[0]),
                TABLE_SORTING_KEY: 'RNTL',
                'rental_id': str(record[1]),
                'rental_date': str(record[2]),
                'inventory_id': int(record[3]),
                'customer_id': int(record[4]),
                'return_date': str(record[5]),
                'staff_id': str(record[6])
            }
        )
    log("Done\n")

    log("Transfer from postgres to DynamoDB completed\n")