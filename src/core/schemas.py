from pyspark.sql.types import StructType, StructField, StringType, IntegerType,TimestampType




new_posts_schema = StructType([
    StructField("data", StructType([
        StructField("post_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("created_at", TimestampType(), True)
    ]))
])

post_share_schema = StructType([
    StructField("data", StructType([
        StructField("post_id", StringType(), True),
        StructField("share_of", StringType(), True),
        StructField("status", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("created_at", TimestampType(), True)
    ]))
])

post_description_edited_schema = StructType([
    StructField("data", StructType([
        StructField("id", StringType(), True),
        StructField("post_id", StringType(), True),
        StructField("description", StringType(), True),
        StructField("created_at", TimestampType(), True)
    ]))
])

delete_post_schema = StructType([
    StructField("data", StructType([
        StructField("id", StringType(), True),
        StructField("post_id", StringType(), True),
        StructField("created_at", TimestampType(), True)
    ]))
])

associate_tag_schema = StructType([
    StructField("data", StructType([
        StructField("id", StringType(), True),
        StructField("post_id", StringType(), True),
        StructField("tag_id", StringType(), True),
        StructField("created_at", TimestampType(), True)
    ]))
])

dissociate_tag_schema = StructType([
    StructField("data", StructType([
        StructField("id", StringType(), True),
        StructField("post_id", StringType(), True),
        StructField("tag_id", StringType(), True),
        StructField("created_at", TimestampType(), True)
    ]))
])

publish_post_schema = StructType([
    StructField("data", StructType([
        StructField("id", StringType(), True),
        StructField("post_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", TimestampType(), True)
    ]))
])

draft_post_schema = StructType([
    StructField("data", StructType([
        StructField("id", StringType(), True),
        StructField("post_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", TimestampType(), True)
    ]))
])

edit_post_publish_time_schema = StructType([
    StructField("data", StructType([
        StructField("id", StringType(), True),
        StructField("post_id", StringType(), True),
        StructField("publish_time", TimestampType(), True),
        StructField("created_at", TimestampType(), True)
    ]))
])
