# Transfer Service

Tranfers all data from a PostgreSQL database to a DynamoDB.

## Dynamo Scheme

The data is organized accordance with the recommendation by the DynamoDB 
documentation to organize data in a single table foundation. This has a couple
of advantages over a more RDB approach, however it can be more challenging to 
understand.

This might at first appear to get rid of any logical structure withing the data
such as relations and hierarchies, however you can still achieve this using a
composite sort key. Because of dynamos primary structure, there can be two 
components to such a key, the partition key and the sort key. For our single 
table approach the partition key will remain the over all rows, the sorting key
however will be a composite of a hierarchical structure that each row might 
posess.

Such a structure typically looks like this: ``` PARENT#CHILD#GRANDCHILD#ETC ```

Using this, we can structure queries over the sorting key, this approach is
similar to navigating a tree structure. For example a very simple relation
would be to include the partition key of a separate type of object as the sort
key for another. Then you can add attributes to that specific row that would be
useful for a query where you are searching for a specific item in relation to
the other.

A more simple way to look at this is to think of primary keys more like 
queries. The partition key defines your entry point similar to a from, and the
sort key includes all the relational information similar to joins. The
attributes at the end allow you to model the select statement.

In summary this is to mean that a dynamo table doesnt primarily model the
relations between your data. It moreso is defined by the access patterns to 
that data, so that rows are more similar to queries rather than data objects.

## Requirements

- Python 3.11.X
- Docker 24.0.X or higher

## Resources

- https://medium.com/@wishmithasmendis/from-rdbms-to-key-value-store-data-modeling-techniques-a2874906bc46
- https://youtu.be/2k2GINpO308
- https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/data-modeling.html
- https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/data-modeling-online-shop.html
- https://www.youtube.com/watch?v=KYy8X8t4MB8