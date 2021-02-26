WebRTC-Observer Report Connector Service
===
![Build](https://github.com/ObserveRTC/connector/actions/workflows/build.yml/badge.svg)
![Test](https://github.com/ObserveRTC/connector/actions/workflows/test.yml/badge.svg)


WebRTC applications integrated with [observer.js](https://github.com/ObserveRTC/integrations) 
forward their reports to the [WebRTC-Observer](https://github.com/ObserveRTC/webrtc-observer) service.
WebRTC-Observer creates [Reports](https://observertc.org/docs/references/reports/).

This service is designed to forward the reports to data warehouses.

## Quick Start

You can deploy the service by using [docker](https://github.com/ObserveRTC/docker-webrtc-observer) 
or [helm](https://github.com/ObserveRTC/helm). 
To forward the Reports from Observer to your warehouse, 
you need to configure a pipeline through a yaml file. 
An example configuration is given in the [docker](https://github.com/ObserveRTC/docker-webrtc-observer) 
deployment.

## Configurations
Under the hood the service creates a simple pipeline 
with three chained components: Source, Evaluator, Sink. 
Different sources, and sinks may have different configurations. 
In the following examples are given in a [yaml](https://www.yaml.io/) format.

Each pipeline has the following configuration structure:

```yaml
name: "The name of my pipeline"
source:
  type: SourceType
  # SourceType specific configurations
  config: {}
mapper:
  # Evaluator configurations
sink:
  type: SinkType
  config: {}
    # SinkType specific configurations
```

### Sources

Under the `source` property
the following configurations you can apply:

#### Kafka
```yaml
  type: KafkaSource
  config:
    # The topic your pipeline will read the data from
    topic: "reports"
    
    # The kafka configuration properties listed in https://kafka.apache.org/0100/documentation.html
    properties:
      bootstrap.servers: localhost:9092
      group.id: "MyGroup"
```


### Evaluators

Under the `mapper` property 
the following configurations you can change

```yaml
    # determines the number of Entries the mapper 
    # holds in the buffer before it sends to the sink
    bufferThresholdNum: 10000 # <- default
    # determines the number of seconds the mapper 
    # wait up until it sends the list of Entries anyway 
    # regardless if it reached the number defined as threshold or not. 
    bufferThresholdInS: 30 # <- default
```

### Sinks

#### BigQuery

```yaml
type: BigQuerySink
config:
  # The IAM credential you obtain from Google BigQuery Service to access 
  # your datawarehouse through the API in a JSON format
  credentialFile: "myCredentials.json"
  
  # The id of the project the credential belongs to
  projectId: "myProject"
  
  # The dataset you want to dump your stats
  datasetId: "WebRTCTry"
  
  # Configuration related to create the schema automatically 
  # for you if it does not exists
  schemaCheck:
    enabled: True
    createTableIfNotExists: True
    createDatasetIfNotExists: True
```

## Compatibility Map

| Report-Connector | v0.2 |
| ---------------- | ----------- |
| Reactor Kafka    | 1.3.1       |

## Develop and contribute

TBD 
