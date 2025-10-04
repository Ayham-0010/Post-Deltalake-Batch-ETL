### Event-Driven Batch Processing with Spark & Delta Lake ### 

This project is an event-driven batch processing pipeline that consumes domain events from EventStoreDB, buffers them in memory, and processes them in micro-batches using Apache Spark. Events are persisted into Delta Lake tables for reliable storage and downstream consumers.

Unlike pure real-time pipelines, this design leverages batch-timeout windows and multi-threading for efficient event handling, making it both scalable and fault-tolerant.

### Key Features ###

* Multi-Threaded Architecture

Subscription thread: continuously consumes events from EventStoreDB.<br>
Processing thread: batches and writes events to Delta Lake.<br>
Decoupled design ensures high throughput and prevents blocking.<br>

* Concurrency Control

Thread-safe event queue with locks & conditions.<br>
Guarantees ordered, consistent processing while allowing concurrent ingestion.<br>

* Event-Driven Batch Processing
Buffers events until a configurable timeout is reached.<br>
Hybrid model: avoids event-at-a-time overhead while staying near real-time.<br>

* Delta Lake for CDC
Tables (post, post_tag) store full history of events with Change Data Capture enabled.<br>
SQL UPDATE and CDC logic ensure that deletes, edits, and status changes are correctly applied.<br>
Enables time travel queries and incremental downstream ETL.<br>

*  Rich Event Handling
Handles core lifecycle events like:<br>
post_created, post_deleted, post_status_changed_to_publish/draft<br>
post_description_edited, publish_time_edited<br>
tag_associated, tag_dissociated<br>

*  Cloud-Native Storage

S3-backed Delta tables for scalability.<br>
Works seamlessly with Spark SQL, ML pipelines, and BI tools.<br>




###  Data Flow ###

* Event Ingestion

The subscription thread connects to EventStoreDB and reads events from the posts stream.<br>
Relevant events (post_created, post_deleted, tag_associated, etc.) are appended to the in-memory queue.<br>

* Batch Accumulation (Time-Based)

Events are continuously buffered in memory.<br>
After the configured time window (TIME_WINDOW_IN_SECONDS) expires, the queue is flushed if not empty.<br>
This design ensures time-based micro-batch processing, reducing overhead compared to per-event writes.<br>

* Batch Processing

The processing thread locks the queue, copies accumulated events, and clears it.<br>
The batch is converted into a Spark DataFrame with strict JSON schemas.<br>


* Delta Lake Storage

Data is written into Delta tables (post/, post_tag/) stored in S3.<br>
CDC is enabled, allowing time travel queries and incremental ETL.<br>

* Downstream Consumption

Stored Delta tables will be queried with Spark SQL, the denormalization pipelines to prepare them for efficient indexing in opensearch.



### Tech Stack ###

Python + PySpark<br>
EventStoreDB<br>
Delta Lakebr>
S3/Cloud Object Storage<br>


