package org.parseplatform.livequery;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.logback.InstrumentedAppender;
import io.sentry.Sentry;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import org.parseplatform.livequery.metrics.StatsDReporter;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class MainVerticle extends AbstractVerticle {

    @Override
    public void start() {
        String sentryDsn = System.getenv("SENTRY_DSN");
        if (sentryDsn != null) {
            Sentry.init((options) -> {
                options.setDsn(sentryDsn);
                options.setServerName(System.getenv("HOSTNAME"));
            });
            vertx.exceptionHandler(Sentry::captureException);
        }

        String appId = getEnv("APP_ID");
        String masterKey = getEnv("MASTER_KEY");
        String serverUrl = getEnv("SERVER_URL");

        String redisUri = System.getenv("REDIS_URI");
        if (redisUri == null) {
            redisUri = "redis://127.0.0.1:6379";
        }

        String portStr = System.getenv("PORT");
        int port;
        if (portStr != null) {
            port = Integer.parseInt(portStr);
        } else {
            port = 8080;
        }

        JsonObject config = new JsonObject().
            put(ConfigKey.PORT, port).
            put(ConfigKey.APP_ID, appId).
            put(ConfigKey.MASTER_KEY, masterKey).
            put(ConfigKey.SERVER_URL, serverUrl).
            put(ConfigKey.REDIS_URI, redisUri);

        vertx.deployVerticle(WebsocketVerticle.class, new DeploymentOptions().setInstances(1).setConfig(config));
        vertx.deployVerticle(RedisSubscriptionVerticle.class, new DeploymentOptions().setInstances(2).setConfig(config));
        vertx.deployVerticle(RedisVerticle.class, new DeploymentOptions().setConfig(config));
        vertx.deployVerticle(UsersVerticle.class, new DeploymentOptions().setConfig(config));

        if (vertx.isMetricsEnabled()) {
            StatsDReporter.forRegistry(getMetricRegistry())
                .build("127.0.0.1", 8125)
                .start(10, TimeUnit.SECONDS);
            instrumentLogger();
        }
    }

    private void instrumentLogger() {
        final LoggerContext factory = (LoggerContext) LoggerFactory.getILoggerFactory();
        final Logger root = factory.getLogger(Logger.ROOT_LOGGER_NAME);
        final InstrumentedAppender metrics = new InstrumentedAppender(getMetricRegistry());
        metrics.setContext(root.getLoggerContext());
        metrics.start();
        root.addAppender(metrics);
    }

    private String getEnv(String key) {
        String val = System.getenv(key);
        if (val == null) {
            throw new RuntimeException("Please set " + key);
        }
        return val;
    }

    static MetricRegistry getMetricRegistry() {
        String registryName = System.getProperty("vertx.metrics.options.registryName");
        return SharedMetricRegistries.getOrCreate(registryName);
    }
}
