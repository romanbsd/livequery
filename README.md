# Parse Livequery server

## General
This is a implementation of the [Parse][1] [LiveQuery][2] server in Java.
This implementation has the following benefits:

* Uses less memory
* Faster

Apart from utilizing the JIT capabilities of the JVM, this server is
substantially faster because it uses a separate registry for "simple" queries.
A "simple" query is a query by one predicate, e.g. equality.
So for most use cases (subscribe to an object by id or by pointer), it will
use the "simple query" registry, which is O(log(n)) vs O(n) for the regular
(by class name) query retrieval.

It was designed as a drop-in replacement for the original Javascript implementation, but no effort was made to follow the changes in the original implementation since then.


## Building

    mvn package
    cp Procfile target/
    pushd target
    zip -u app.zip Procfile livequery-1.0.0-SNAPSHOT-fat.jar
    popd


## Configuration

The server is configured using the following environment variables:

* APP_ID (required) - Parse Application Id
* MASTER_KEY (required) - Parse Server master key
* SERVER_URL (required) - Parse Sever URL
* REDIS_URI (optional, default: redis://127.0.0.1:6379)
* PORT (optional, default: 8080) - WebSocket listening port
* SENTRY_DSN (optional) - Set it if you're using [Sentry][3].
  Sentry depends on HOSTNAME environment variable, so make sure that
  it's set correctly.


## Logging
The logging is performed using Logback / SLF4J, you can tweak the configuration in:

    src/main/resources/logback.xml

## Metrics collection

It's possible to enable metrics collection by editing Procfile and changing

    -Dvertx.metrics.options.enabled=true

When enabled, metrics will be delivered to a local StatsD compatible service (running on 127.0.0.1:8125) every 10 seconds.


## Deployment on AWS Elastic Beanstalk

Create a Java 8 compatible instance using the regular EB procedure.
It will probably work with Java 11 or Corretto, but I never tried it.

Add to .elasticbeanstalk/config.yml:

    deploy:
      artifact: target/app.zip

Deploy with eb cli:

    eb deploy --staged


[1]: https://parseplatform.org/
[2]: https://docs.parseplatform.org/parse-server/guide/#live-queries
[3]: https://sentry.io/
