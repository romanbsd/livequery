package org.parseplatform.livequery;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.redis.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class RedisVerticle extends AbstractVerticle {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisVerticle.class);

    @Override
    public void start(Promise<Void> startPromise) {
        final String redisUri = config().getString(ConfigKey.REDIS_URI);
        Redis.createClient(vertx, redisUri).connect(onConnect -> {
            if (onConnect.succeeded()) {
                final AtomicInteger count = new AtomicInteger();
                final RedisConnection connection = onConnect.result();
                connection.handler(response -> {
                    if (!(response.type() == ResponseType.MULTI && "subscribe".equals(response.get(0).toString()))) {
                        return;
                    }
                    String channel = response.get(1).toString();
                    LOGGER.info("Subscribed to {}", channel);
                    if (count.incrementAndGet() == 2) {
                        LOGGER.debug("Subscribed to both channels");
                        connection.handler(null);
                        startPromise.complete();
                    }
                });
                String appId = config().getString(ConfigKey.APP_ID);
                final String afterSave = appId + ParseConstants.AFTER_SAVE;
                final String afterDelete = appId + ParseConstants.AFTER_DELETE;
                connection.send(Request.cmd(Command.SUBSCRIBE).arg(afterSave).arg(afterDelete), onSubscribe -> {
                    if (onSubscribe.failed()) {
                        LOGGER.error("Subscription failed", onSubscribe.cause());
                        startPromise.fail(onSubscribe.cause());
                    }
                });
            } else {
                LOGGER.error("Connection failed", onConnect.cause());
                startPromise.fail(onConnect.cause());
            }
        });
    }
}
