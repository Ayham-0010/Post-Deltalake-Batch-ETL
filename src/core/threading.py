import threading
import time
from collections import namedtuple

from pyspark.sql.functions import lit
from pyspark.sql.functions import from_json, col, max as spark_max
from pyspark.sql import Row
from esdbclient import *

from .schemas import *
from .consts import *
from .logger import logger




Row = namedtuple('Row', ['position', 'data', 'type'])

class EventProcessor:
    def __init__(self,spark, client, group_name, stream_name, batch_size, time_out_after):
        self.spark = spark
        self.client = client
        self.group_name = group_name
        self.stream_name = stream_name
        self.events_list = []
        self.batch_size = batch_size
        self.time_out_after = time_out_after
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        self.last_batch_time = time.time()

    def run_subscription(self):
        
        while True:

                subscription = self.client.read_subscription_to_stream(
                    group_name=self.group_name,
                    stream_name=self.stream_name
                    
                )
                for event in subscription:

                        if event.type in [ "post_created", "post_share_created", "post_deleted", "post_description_edited", "post_status_changed_to_publish","tag_associated", "tag_dissociated"]:
                            event_data = event.data.decode('utf-8')
                            event_position = event.stream_position
                            event_type = event.type
                            
                            logger.info(f"Event position: {event_position}")
                            
                
                            
                            self.append_event(Row(position=event_position, data=event_data, type=event_type))
                            subscription.ack(event)
                        else:
                            subscription.ack(event)
   

    def append_event(self, event):
        logger.info(f"append event {event}")
        with self.lock:
            self.events_list.append(event)


    def process_events(self):
        
        while True:
            


            if self.events_list and len(self.events_list) > 0:


                time.sleep(self.time_out_after)
                logger.info(f"timeout!")
                with self.lock:    
                   
                    events_to_process = self.events_list.copy()
                    self.handle_events(events_to_process)
                    self.events_list = []
                    logger.info(f"events list cleared!")




    def handle_insert(self, df, path, type):
        logger.info(f"handle_insert started")
        df.write.format("delta").mode("append").save(path)
        




    def handle_update_delete(self, df, type):
         
        if type == 'post_description_edited':
              df.show()



              max_positions = df.groupBy("post_id").agg(spark_max("position").alias("max_position")).collect()
              max_positions_list = [row["max_position"] for row in max_positions]
              filtered_df = df.filter(df["position"].isin(max_positions_list))

              filtered_df.show()
              data = filtered_df.collect()
        else:
              data = df.collect()
            
        for row in data:   
            
            if type == 'post_deleted':
                        

                    self.spark.sql(f" UPDATE post SET deleted_at = '{row.deleted_at}' WHERE post_id = '{row.post_id}'; ")
        
            if type in ['post_status_changed_to_publish', 'post_status_changed_to_draft'] :
            

                    self.spark.sql(f" UPDATE post SET status = '{row.status}' WHERE post_id = '{row.post_id}'; ")
                
            if type == 'publish_time_edited':
                        
                
                    self.spark.sql(f" UPDATE post SET publish_at = '{row.publish_at}' WHERE post_id = '{row.post_id}'; ")
                
            if type == 'post_description_edited':
                    
                    description= row.description
                    escabed_description= description.translate(str.maketrans({"'":  r"^","'":  r"^","\\":  r"^"}))
                
                    self.spark.sql(f" UPDATE post SET description = '{escabed_description}' WHERE post_id = '{row.post_id}'; ")
                
            if type == 'tag_dissociated':

                    self.spark.sql(f" UPDATE post SET deleted_at = '{row.deleted_at}' WHERE post_id = '{row.post_id}' AND tag_id = '{row.tag_id}'; ")


    def handle_events(self, events_list):

        if not events_list:
            return
        df = self.spark.createDataFrame(events_list)

        
        # post_created

        new_posts_df = df.filter(col("type") == 'post_created')
        new_posts_df = new_posts_df.select(
            col('position'),
            from_json(col("data"), new_posts_schema).alias("value"),
            col('type')
        )
        new_posts_df = new_posts_df.select(
            col("value.data.post_id").alias("post_id"),
            col("value.data.status").alias("status"),
            col("value.data.account_id").alias("account_id"),
            col("value.data.created_at").alias("created_at"),
        )
        
        self.handle_insert(new_posts_df,content_delta_tables_base_path + post_delta_tables_path,'post_created')


        # post_share_created
    
        new_post_shares_df = df.filter(col("type") == 'post_share_created')
        new_post_shares_df = new_post_shares_df.select(
            col('position'),
            from_json(col("data"), post_share_schema).alias("value"),
            col('type')
        )
        new_post_shares_df = new_post_shares_df.select(
            col("value.data.post_id").alias("post_id"),
            col("value.data.account_id").alias("account_id"),
            col("value.data.status").alias("status"),
            col("value.data.share_of").alias("share_of"),
            col("value.data.created_at").alias("created_at"),
            
        )
        
        self.handle_insert(new_post_shares_df,content_delta_tables_base_path + post_delta_tables_path, 'post_share_created')


        #post_deleted
    
        delete_posts_df = df.filter(col("type") == 'post_deleted')
        delete_posts_df = delete_posts_df.select(
            col('position'),
            from_json(col("data"), delete_post_schema).alias("value"),
            col('type')
        )
        delete_posts_df = delete_posts_df.select(
            col("value.data.post_id").alias("post_id"),
            col("value.data.created_at").alias("deleted_at"),
            
        )
        
        self.handle_update_delete(delete_posts_df, 'post_deleted')


        # post_description_edited
    
        post_description_edited_df = df.filter(col("type") == 'post_description_edited')
        post_description_edited_df = post_description_edited_df.select(
            col('position'),
            from_json(col("data"), post_description_edited_schema).alias("value"),
            col('type')
        )
        post_description_edited_df = post_description_edited_df.select(
            col("position").alias("position"),
            col("value.data.post_id").alias("post_id"),
            col("value.data.description").alias("description"),
            col("value.data.created_at").alias("updated_at"),
            
        )

        self.handle_update_delete(post_description_edited_df, 'post_description_edited')

    
        # post_status_changed_to_publish
    
        publish_posts_df = df.filter(col("type") == 'post_status_changed_to_publish')
        publish_posts_df = publish_posts_df.select(
            col('position'),
            from_json(col("data"), publish_post_schema).alias("value"),
            col('type')
        )
        publish_posts_df = publish_posts_df.select(
            col("value.data.post_id").alias("post_id"),
            col("value.data.status").alias("status"),
        )

        self.handle_update_delete(publish_posts_df, 'post_status_changed_to_publish')


    
        # tag_associated
    
        add_tags_df = df.filter(col("type") == 'tag_associated')
        add_tags_df = add_tags_df.select(
            col('position'),
            from_json(col("data"), associate_tag_schema).alias("value"),
            col('type')
        )
        add_tags_df = add_tags_df.select(
            col("value.data.post_id").alias("post_id"),
            col("value.data.tag_id").alias("tag_id"),
            
        )
        
        self.handle_insert(add_tags_df, content_delta_tables_base_path + post_tag_delta_tables_path, 'tag_associated')

    
        # tag_dissociated
    
        delete_tags_df = df.filter(col("type") == 'tag_dissociated')
        delete_tags_df = delete_tags_df.select(
            col('position'),
            from_json(col("data"), dissociate_tag_schema).alias("value"),
            col('type')
        )
        delete_tags_df = delete_tags_df.select(
            col("value.data.post_id").alias("post_id"),
            col("value.data.tag_id").alias("tag_id"),
            col("value.data.created_at").alias("deleted_at"),
            
        )
        
        self.handle_update_delete(delete_tags_df, 'tag_dissociated')





    def get_events_list_safe(self):
        with self.lock:
            # Return a copy of the list to prevent modification while being accessed
            return self.events_list.copy()