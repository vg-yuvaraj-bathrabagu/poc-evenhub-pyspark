from pyspark.sql import SparkSession

def initialize_spark():
    return SparkSession.builder.appName("AzureEventHubDynamicSchema").getOrCreate()

def send_partition(event_hub_connection_str, event_hub_name, iterator):
    from azure.eventhub import EventHubProducerClient, EventData
    producer = EventHubProducerClient.from_connection_string(event_hub_connection_str, eventhub_name=event_hub_name)
    batch = producer.create_batch()

    for json_str in iterator:
        event_data = EventData(json_str)
        try:
            batch.add(event_data)
        except ValueError:
            producer.send_batch(batch)
            batch = producer.create_batch()
            batch.add(event_data)

    if len(batch) > 0:
        producer.send_batch(batch)
    
    producer.close()

def main():
    spark = initialize_spark()

    # Set up Event Hub configurations
    event_hub_connection_str = "YOUR_EVENT_HUB_CONNECTION_STRING"
    event_hub_name = "YOUR_EVENT_HUB_NAME"

    # Read JSON data as text
    file_path = "data/*.json"
    sample_df = spark.read.json("data/json_data.json", multiLine=True)
    sample_df.show()
    inferred_schema = sample_df.schema
    streaming_df = spark.readStream \
        .format("json") \
        .schema(inferred_schema) \
        .option("multiLine", "true") \
        .load(file_path)

    # streaming_df.show()

    # # Write the data to Event Hub asynchronously
    
    selected_df = streaming_df.select("*")

    # Write the data to Event Hub asynchronously
    query = selected_df.writeStream \
        .foreachBatch(lambda batch_df, batch_id: send_partition(event_hub_connection_str, event_hub_name, batch_df.toJSON().collect())) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
