WebRTC-Observer Report Connector Service
==
![Build](https://github.com/ObserveRTC/connector/actions/workflows/build.yml/badge.svg)
![Test](https://github.com/ObserveRTC/connector/actions/workflows/test.yml/badge.svg)

This service is designed to consume [WebRTC-Reports](https://observertc.org/docs/references/reports/), 
transform and forward them into databases. 

## Dependencies

Describe any dependencies that must be installed for this software to work.
This includes programming languages, databases or other storage mechanisms, build tools, frameworks, and so forth.
If specific versions of other software are required, or known not to work, call that out.

## Installation

Please read [INSTALL](INSTALL.md) instructions.   

## Configuration

The application uses the [micronaut](micronaut.io) framework, 
the configuration is fetched from that.

## Usage

The service executes pipelines. 
Pipelines can be setup by giving JSON or YAML configurations.
One example for a pipeline configuration is given below:

```yaml
name: "MyPipeline"
source:
  type: FileSource
  config:
    path: "path/to/avro_files"
decoder:
  type: AvroDecoder
buffer:
  maxItems: 100
  maxWaitingTimeInS: 10
sink:
  type: LoggerSink
```
This pipeline can be saved into a `test-pipeline.yaml`, from 
which the connector can manifest an actual pipeline 
by parsing it from `PIPELINE_CONFIG_FILES` env variables.


## How to test the software

The service uses `gradle` to build and test.
To simply run tests: `gradle test`

## Known issues

TBD

## Getting help

If you have questions, concerns, bug reports, etc, please file an issue in this repository's Issue Tracker.

## Getting involved

We currently focusing on the following areas of development this service:
 * Better documentation
 * Improve test coverage
 * Add new type of sink components

If you would like to contribute, first of all many thanks, 
second of all, please read [CONTRIBUTING](CONTRIBUTING.md) guidline.

----

## Open source licensing info

1. [LICENSE](LICENSE)

----

## Credits and references

1. Projects that inspired you
2. Related projects
3. Books, papers, talks, or other sources that have meaningful impact or influence on this project

## Observer Compatibility


| observer :arrow_down: connector:arrow_forward: | 0.1.9 | 0.1.7 | 0.1.6 | 0.1.5 | 0.1.4 |
|------------------------------------------------|-------|-------|-------|-------|-------|
| 0.8.2                                          | :ok:  | :x:   | :x:   | :x:   | :x:   |
| 0.8.0                                          | :ok:  | :x:   | :x:   | :x:   | :x:   |
| 0.7.0                                          | :x:   | :ok:  | :ok:  | :ok:  | :x:   |
| 0.6.5                                          | :x:   | :ok:  | :ok:  | :ok:  | :x:   |
| 0.6.4                                          | :x:   | :x:   | :x:   | :x:   | :ok:  |
