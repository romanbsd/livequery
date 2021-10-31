package org.parseplatform.livequery;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static org.parseplatform.livequery.ParseConstants.AFTER_DELETE;
import static org.parseplatform.livequery.ParseConstants.AFTER_SAVE;

// Redis subscriber
public class RedisSubscriptionVerticle extends AbstractVerticle {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisSubscriptionVerticle.class);
    private static final String BASE = "io.vertx.redis.";
    private final SubscriptionRegistry subscriptionRegistry = SubscriptionRegistry.getInstance();

    @Override
    public void start() {
        String appId = config().getString(ConfigKey.APP_ID);
        vertx.eventBus().consumer(BASE + appId + AFTER_SAVE, this::onAfterSave);
        vertx.eventBus().consumer(BASE + appId + AFTER_DELETE, this::onAfterDelete);
    }

    private void onAfterSave(Message<JsonObject> message) {
        LOGGER.debug("onAfterSave {}", message.body());
        ParseMessage o = ParseMessage.create(message);
        Set<Subscription> subscriptions = subscriptionRegistry.findSubscriptions(o);
        for (Subscription subscription : subscriptions) {
            subscription.afterSave(o, vertx.eventBus());
        }
    }

    private void onAfterDelete(Message<JsonObject> message) {
        LOGGER.debug("onAfterDelete {}", message.body());
        ParseMessage o = ParseMessage.create(message);
        Set<Subscription> subscriptions = subscriptionRegistry.findSubscriptions(o);
        for (Subscription subscription : subscriptions) {
            subscription.afterDelete(o, vertx.eventBus());
        }
    }

}
