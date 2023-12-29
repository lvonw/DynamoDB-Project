import  psycopg
import  transfer
import  queries

from    constants   import *
from    logger      import log
from    psycopg     import OperationalError
from    contextlib  import contextmanager


# ========== PREFACE ===================================================
# As we decided to follow the best practice, and recommendation of the 
# DynamoDB documentation we utilize the single table design scheme.
# Further, the documentation for data modelling in DynamoDB advises to
# model the data to best suit the access patterns for it, rather than to
# reflect the relations between the items as one might find in an RDB. 
# To this end we defined our access patterns as the tasks layed out by
# the assignment and designed our database accordingly.
# ======================================================================

log("========== SCRIPT STARTED ====================================")
@contextmanager
def create_conn():
    conn = None
    while not conn:
        log("Trying to connect to database")
        try:
            conn = psycopg.connect(
                dbname     = POSTGRES_DB_NAME, 
                user       = POSTGRES_DB_USER, 
                password   = POSTGRES_DB_PASSWORD,
                host       = POSTGRES_DB_HOST,
                port       = POSTGRES_DB_PORT)
            log("Database connection successful\n")
            yield conn # Transfers control to context manager ('with ... as ...')
        except OperationalError as e:
            log(e)
            time.sleep(5)

# Create postgres db connection
with create_conn() as conn, conn.cursor() as cur:

    # Reduce nummber of table rows for testing
    if REDUCE_TABLE_ROWS:
        log("Reducing large postgres tables to " + str(REDUCTION_GOAL) + " rows")
        
        # Delete from association tables
        for table in ASSOCIATION_TABLES_TO_REDUCE:
            cur.execute(f"SELECT * FROM {table[0]};")
            rows = cur.fetchall()
            total_rows = len(rows)
            rows_deleted = 0
            failed_deletes = 0
            for row in rows:

                # Terminate if minimum amount of rows is reached
                if total_rows - rows_deleted <= REDUCTION_GOAL:
                    break
                
                # Delete row
                try:                    
                    cur.execute(f"DELETE FROM {table[0]} WHERE \
                            {table[1]} = %s AND {table[2]} = %s;", \
                            (row[0], row[1]))
                    conn.commit()
                    rows_deleted += 1

                except psycopg.IntegrityError as e:
                    conn.rollback()
                    failed_deletes += 1
            log("Deleted " + str(rows_deleted) + " from association table " + table[0] \
                + ", " + str(failed_deletes) + " failed delete attempts")

        # Delete from regular tables
        for table in TABLES_TO_REDUCE:
            cur.execute(f"SELECT * FROM {table};")
            rows = cur.fetchall()
            total_rows = len(rows)
            rows_deleted = 0
            failed_deletes = 0
            for row in rows:
                # Terminate if minimum amount of rows is reached
                if total_rows - rows_deleted <= REDUCTION_GOAL:
                    break

                # Delete row
                try:
                    cur.execute(f"DELETE FROM {table} WHERE \
                                {table + '_id'} = %s;", (row[0],)) # row[0] = primary key value
                    conn.commit()
                    rows_deleted += 1

                except psycopg.IntegrityError as e:
                    conn.rollback()
                    failed_deletes += 1
            log("Deleted " + str(rows_deleted) + " from table " + table \
                + ", " + str(failed_deletes) + " failed delete attempts")

        log("Table reduction complete\n")

    # Run main scripts
    if CREATE_DYNAMOBD_TABLES:
        transfer.run_transfer(conn, cur)
    
    queries.run_queries(conn)

    
    