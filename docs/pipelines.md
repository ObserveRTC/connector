Pipelines
===

Pipelines are the basis of `connector` service. 
The `connector` service can capable of running several pipelines 
parallely.

# Submitting pipeline

In the current state of the service, pipelines 
can be configured and submitted to the connector 
at startup.

pipeline configs are fetched either from `application.yaml`:
```yaml
connector:
  pipelines:
    - {} # Config of the actual pipeline
```

or either given in separated `yaml` files in 
`PIPELINE_CONFIG_FILES`.  (i.e.: `PIPELINE_CONFIG_FILES=/path/to/my/pipeline.yaml`)


# Configurations

The basic structure to config a pipeline is given below

```yaml
name: "MyPipeline"
meta: # optional, default values below
  description: "No description is given for this pipeline"
  replicas: 1
source: # required
  type: "Source"
  config: # The specific config belongs to the type of source
    key: value
decoder: # optional, default below:
  type: org.observertc.webrtc.decoders.AvroDecoder
  config: {}
transformations: [] # optional, by default it is an empty array
buffer: # optional, default configuration below
  maxItems: 1000
  maxWaitingTimeInS: 30
sink: # required
  type: "Sink"
  config: # The specific config belongs to the type of sink
    key: value
```
The two required components are Source, and Sink, every other component is optional.

## Meta

Meta configuration for a pipeline. 
Replicas will tell the `connector` how many pipeline 
need to be initiated with the same configuration.

```yaml
meta:
  description: "Your longer description of what the pipeline is doing"
  replicas: 1 # default
```

## Sources

Sources are required to provided for every pipeline 
configured for the `connector`. The sources describes 
a data provider the `connector` fetches reports from.

### Kafka

```yaml
source: 
  type: "Kafka"
  config: 
    topic: "topicName"
    properties:
      # https://kafka.apache.org/documentation/#consumerconfigs
      bootstrap.servers: localhost:9092
      group.id: "test-something"

```

### File

```yaml
source: 
  type: "File"
  config: 
    path: "/path/to/directory"
```

## Decoders

Decoders convert to incoming bytestream to 
Avro Reports. In normal situation the default 
decoder assumes the bytestream without further transformation 
contains the Avro format. However, if additional 
operation (i.e.: decryption) is required to extract 
the bytestream and convert it to the format you can 
provide the class for that

```yaml
decoders:
  type: your.package.YourClass
```

The class you provide must have a constructor without 
any parameters, and that class must implement 
`org.observertc.webrtc.decoders.Decoder` interface.


### Avro Decoder

```yaml
decoders:
  type: org.observertc.webrtc.decoders.AvroDecoder
```

## Transformations

Transformations are applied on Reports  

### ReportObfuscator

### Filter

## Buffers

## Sinks

### BigQuery

