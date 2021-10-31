package org.parseplatform.livequery;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Subscription {
    private static final Logger LOGGER = LoggerFactory.getLogger(Subscription.class);
    private final Query query;
    private final Map<Client, Set<Integer>> clientRequestIds = new HashMap<>();

    public Subscription(Query query) {
        this.query = query;
    }

    public String className() {
        return query.className;
    }

    public void addClientSubscription(Client client, Integer requestId) {
        Set<Integer> requestIds = clientRequestIds.computeIfAbsent(client, k -> new HashSet<>());
        requestIds.add(requestId);
    }

    public void removeClientSubscription(Client client, Integer requestId) {
        Set<Integer> requestIds = clientRequestIds.get(client);
        if (requestIds == null) {
            return;
        }
        requestIds.remove(requestId);
        if (requestIds.isEmpty()) {
            clientRequestIds.remove(client);
        }
    }

    public boolean hasSubscribingClient() {
        return !clientRequestIds.isEmpty();
    }

    public boolean matches(ParseMessage message) {
        return query.matches(message.currentParseObject) || query.matches(message.originalParseObject);
    }

    public void afterSave(ParseMessage message, EventBus eventBus) {
        boolean isCurrentMatched = query.matches(message.currentParseObject);
        boolean isOriginalMatched = query.matches(message.originalParseObject);

        Event event;
        if (isOriginalMatched && isCurrentMatched) {
            event = Event.update;
        } else if (isOriginalMatched) {
            event = Event.leave;
        } else if (isCurrentMatched) {
            if (message.originalParseObject == null) {
                event = Event.create;
            } else {
                event = Event.enter;
            }
        } else {
            LOGGER.error("afterSave: query doesn't match");
            return;
        }
        LOGGER.debug("afterSave | ClassName: {} | ObjectId: {} | Event: {}", message.className, message.objectId, event);
        clientRequestIds.forEach((client, value) -> {
            for (Integer requestId : value) {
                checkACL(eventBus, message, client, requestId).onComplete((ar) -> {
                    if (ar.result()) {
                        client.pushEvent(event, requestId, message.currentParseObject);
                    } else {
                        LOGGER.debug("not forwarding message to {}", client.getId());
                    }
                });
            }
        });
    }

    public void afterDelete(ParseMessage message, EventBus eventBus) {
        LOGGER.debug("afterDelete | ClassName: {} | ObjectId: {}", message.className, message.objectId);

        if (!query.matches(message.currentParseObject)) {
            LOGGER.error("not matching message");
            return;
        }
        clientRequestIds.forEach((client, value) -> {
            for (Integer requestId : value) {
                checkACL(eventBus, message, client, requestId).onComplete((ar) -> {
                    if (ar.result()) {
                        client.pushEvent(Event.delete, requestId, message.currentParseObject);
                    } else {
                        LOGGER.debug("not forwarding message to {}", client.getId());
                    }
                });
            }
        });
    }

    private Future<Boolean> checkACL(EventBus eventBus, ParseMessage message, Client client, Integer requestId) {
        final Promise<Boolean> promise = Promise.promise();
        if (message.isPubliclyReadable()) {
            promise.complete(true);
            return promise.future();
        }
        SubscriptionInfo info = client.getSubscriptionInfo(requestId);
        String token;
        if (info != null && (token = info.getSessionToken()) != null) {
            eventBus.request(UsersVerticle.class.getName(), token, ar -> {
                if (ar.succeeded()) {
                    String userId = (String) ar.result().body();
                    promise.complete(message.isReadableBy(userId));
                } else {
                    LOGGER.warn("cannot check ACL", ar.cause());
                    promise.complete(false);
                }
            });
        } else {
            promise.complete(false);
        }
        return promise.future();
    }

    @Override
    public int hashCode() {
        return query.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Subscription)) {
            return false;
        }
        return query.equals(((Subscription) obj).query);
    }

    public Query getQuery() {
        return query;
    }
}
