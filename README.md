# transformation-instance

Transformation Instance is an open source, light weight and server side collector that provides customers with abilities to filter, enrich, trasform, normalize and aggregate data for analytics. The primary component of transformation instance is a **pipeline**.

## Pipeline
A TI pipeline has four key components **source**, **buffer**, **processor**, and **sink**. A single instance of TI can have one or more pipelines. A pipeline definition contains two required components **source** and **sink**. The other components *viz* **buffer** and **processor** are optional and will default to default implementations in the absence of user implementation.

### Source
Source is the input component of a pipeline, it defines the mechanism through which a TI pipeline will consume records. Source component could consume records either by receiving over http/s or reading from external endpoints like Kafka, SQS, Cloudwatch etc.  Source will have its own configuration options based on the type like the format of the records (string/json/cloudwatch logs/open telemetry trace) , security, concurrency threads etc . The source component will consume records and write them to the buffer component.

### Buffer
The buffer component will act as the layer between the *source* and *sink.* The buffer could either be in-memory or disk based. The default buffer will be in-memory queue bounded by the number of records/size of records. 

### Sink
Sink in the output component of pipeline, it defines the one or more destinations to which a TI pipeline will publish the records. A sink destination could be either services like elastic search, s3 or another TI pipeline. By using another TI pipeline as sink, we could chain multiple TI pipelines. Sink will have its own configuration options based on the destination type like security, request batching etc. 

### Processor
Processor component of the pipeline, these are intermediary processing units using which users can filter, transform and enrich the records into desired format before publishing to the sink. The processor is an optional component of the pipeline, if not defined the records will be published in the format as defined in the source.