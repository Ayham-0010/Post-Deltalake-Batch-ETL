import os
import sys
import time

from pyspark.sql import SparkSession, Row
from pyspark import SparkContext
import pyspark
from esdbclient import *
from delta import *
import esdbclient
#from core.post_batch_job_to_delta_lake import *
from core.logger import logger
from core.threading import *





def main():
    

    while True:
        try:

            logger.info("service started successfully")
            # stream_name="posts"
            # group_name="post_group_1"

            stream_name = os.environ['STREAM_NAME']
            group_name=os.environ['GROUP_NAME']

            
            content_delta_tables_base_path = "s3a://mdcin-delta-lake/mdcin_dev/"
            post_delta_tables_path = "post/"
            post_tag_delta_tables_path = "post_tag/"
            # posts_eventstore_checkpoint = "checkpoints/posts_eventstore_checkpoint/"
            
            # events_batch_size = 100
            # timeout = 30
                
            events_batch_size = int(os.environ['EVENTS_BATCH_SIZE'])
            timeout = int(os.environ['TIME_WINDOW_IN_SECONDS'])

            # spark_connect_url = "sc://spark-master-svc.data-lab-system:15002"

            #kafka_servers = "kafka-kafka-bootstrap.data-lab-system:9092"
            #spark = create_spark_remote_session(spark_connect_url)
            logger.info("creating spark remote session")
            # spark = SparkSession.builder.remote(spark_connect_url).getOrCreate()
            spark = (
                    SparkSession.builder
                    .appName("post_eventstore_to_deltalake_app")
                    .config("spark.streaming.stopGracefullyOnShutdown", True)
                    .getOrCreate()
                )
            # logger.info("creating eventstoredb leader client")
            # client_leader = EventStoreDBClient(
            #     uri="esdb+discover://eventstore.eventstore-test-system:2113?tls=false&keepAliveTimeout=10000&keepAliveInterval=10000&nodepreference=leader"
            # )

            logger.info("creating eventstoredb client")
            client = EventStoreDBClient(
                uri="esdb+discover://eventstore.eventstore-test-system:2113?tls=false&keepAliveTimeout=10000&keepAliveInterval=10000&nodepreference=leader"
            )

            # logger.info("creating eventstoredb subscription to posts stream")
            # client_leader.create_subscription_to_stream(
            #     group_name = group_name,
            #     stream_name = stream_name,
            # )

            # delta tables
            
            #post_delta_table = DeltaTable.forPath(spark, content_delta_tables_base_path + post_delta_tables_path)
            
            #post_tag_delta_table = DeltaTable.forPath(spark, content_delta_tables_base_path + post_tag_delta_tables_path)
            logger.info("enabling CDC in new delta tables")
            spark.sql(f"set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;")
            logger.info("adding post delta table to spark sql catalog")
            spark.sql(f"CREATE TABLE IF NOT EXISTS post USING DELTA LOCATION '{content_delta_tables_base_path + post_delta_tables_path}'")
            logger.info("adding post_tag delta table to spark sql catalog")
            spark.sql(f"CREATE TABLE IF NOT EXISTS post_tag USING DELTA LOCATION '{content_delta_tables_base_path + post_tag_delta_tables_path}'")


            #check_point
            # spark.sql(f"CREATE TABLE IF NOT EXISTS posts_checkpoints USING DELTA LOCATION '{content_delta_tables_base_path + posts_eventstore_checkpoint}'")


                    
            # latest_checkpoint= spark.sql(f"select * from posts_checkpoints limit 1;").collect()[0]['posts_ckp']
            # logger.info(f"posts_checkpoint: {latest_checkpoint}")
            # latest_checkpoint = 0



            logger.info("event batch processing started")

            processor = EventProcessor(spark, client, group_name, stream_name, batch_size=events_batch_size, time_out_after=timeout)

            subscription_thread = threading.Thread(target=processor.run_subscription)
            subscription_thread.start()

            processing_thread = threading.Thread(target=processor.process_events)
            processing_thread.start()


            processing_thread.join()
            subscription_thread.join()




            #last_batch_time=time.time()

            #events_list = []
        #     while True:
        #         subscription = client.read_subscription_to_stream(
        #             group_name= group_name,
        #             stream_name= stream_name
        #         )
                

        #         for event in subscription:

        #             try:

        #                 event_data = event.data.decode('utf-8')
        #                 event_position = event.stream_position
        #                 event_type = event.type
        #                 #logger.info(f" event_position = {event_position}  i = {i}")
                            
        #                 if event_position < i:
        #                     logger.info(f"continue")
        #                     subscription.ack(event)
        #                     continue
                        
        #                 elif event_position == i:
        #                     i = i+1

        #                     spark.sql(f"UPDATE posts_checkpoints SET posts_ckp = posts_ckp + 1;")
        #                     #logger.info(f"i = i+1    i = {i}")
        #                     #current_time = time.time()                
        #                     #events_list.append(Row(position=event.stream_position, data=event_data, type=event.type))
                            
        #                     logger.info(f"event number {event.stream_position} sent for processing.")





        #                     #current_time = time.time()
        #                     #if len(events_list) >= 20 or (current_time - last_batch_time) >= 20:
        #                     handle_events(spark, event_data, event_position, event_type)
        #                     #events_list = []
        #                     logger.info(f"event number {event.stream_position} processed successfully")
        #                     subscription.ack(event)
        #                     #last_batch_time = current_time

        #             except Exception as e:
        #                 print(f"Exception occurred: {e}")
        #                 #events_list.pop()
        #                 if event.retry_count < 5:
        #                     subscription.nack(event, action="retry")
        #                 else:
        #                     subscription.nack(event, action="park")

        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            continue



if __name__ == "__main__":
    main()
