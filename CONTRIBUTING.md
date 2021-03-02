# Contributing Code or Documentation to Micronaut

## Finding Issues to Work on

If you are interested in contributing to ObserveRTC 
and are looking for issues to work on, take a look at the issues  
tagged with [help wanted](https://github.com/ObserveRTC/connector/labels/help%20wanted).

## JDK Setup

The application currently requires JDK 11

## IDE Setup

The application can be imported into IntelliJ IDEA by opening the `build.gradle` file.

## Docker Setup

Docker image is published for each released version to [Dockerhub](https://hub.docker.com/repository/docker/observertc/connector) 

## Running Tests

To run the tests use `gradle test`.

## Building Documentation

The documentation sources are located at `docs/`.

## Working on the code base

If you are working with the IntelliJ IDEA development 
environment, you can import the project using the Intellij 
Gradle Tooling ( "File / Import Project" and select the 
"settings.gradle" file).

To get a local development version of the service working, 
first run the `build` task.

```
./gradlew build
```

The service itself is to apply pipelines and forward WebRTC-Reports 
produced by another service. To develop this service first, it 
is recommended to run another service (i.e.: [observer](https://github.com/ObserveRTC/observer)) 
and setup your integration with it. For local development see 
a [docker example](https://github.com/ObserveRTC/docker-webrtc-observer), 
where the connector is used as an image. Try run the fullstack without 
the connector service and run your local development instance.

Once the connector is in place to consume WebRTC-Reports 
you need to setup the pipeline for your needs and development.
For development check out the 
[development manual](https://observertc.github.io/connector/). 

## Creating a pull request

Once you are satisfied with your changes:

- Commit your changes in your local branch
- Push your changes to your remote branch on GitHub
- Send us a [pull request](https://help.github.com/articles/creating-a-pull-request)

## Checkstyle

We want to keep the code clean,
following good practices about organization, javadoc and 
style as much as possible.
