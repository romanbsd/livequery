package org.parseplatform.livequery;

import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.parseplatform.livequery.ParseConstants.OBJECT_ID;

public class SubscriptionRegistry {
    private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionRegistry.class);
    static final String TYPE = "__type";
    static final String POINTER = "Pointer";
    private static SubscriptionRegistry sInstance;
    // className -> classSubscriptions
    private final Map<String, Map<Query, Subscription>> subscriptionsByClass = new ConcurrentHashMap<>();
    private final SimpleQueryRegistry simpleQueryRegistry = new SimpleQueryRegistry();

    private SubscriptionRegistry() {
    }

    public static SubscriptionRegistry getInstance() {
        synchronized (SubscriptionRegistry.class) {
            if (sInstance == null) {
                sInstance = new SubscriptionRegistry();
            }
            return sInstance;
        }
    }

    public int count() {
        int count = simpleQueryRegistry.count();
        for (Map<Query, Subscription> querySubscriptionMap : subscriptionsByClass.values()) {
            count += querySubscriptionMap.size();
        }
        return count;
    }

    public void removeIfEmpty(Subscription subscription) {
        if (subscription == null || subscription.hasSubscribingClient()) {
            return;
        }
        Query query = subscription.getQuery();
        if (query.isSimple()) {
            simpleQueryRegistry.removeIfEmpty(subscription);
        }
        Map<Query, Subscription> subscriptionMap = subscriptionsByClass.get(subscription.className());
        if (subscriptionMap != null) {
            subscriptionMap.remove(query);
        }
    }

    public Subscription findOrCreate(Query query) {
        if (query.isSimple()) {
            return simpleQueryRegistry.findOrCreate(query);
        }
        Map<Query, Subscription> classSubscriptions = subscriptionsByClass.computeIfAbsent(query.className, s -> new ConcurrentHashMap<>());
        return classSubscriptions.computeIfAbsent(query, Subscription::new);
    }

    // attributes -> map of [attrValue -> subscriberList]
    public Set<Subscription> findSubscriptions(ParseMessage o) {
        Set<Subscription> subscriptions = simpleQueryRegistry.findSubscriptions(o);
        if (!subscriptions.isEmpty()) {
            return subscriptions;
        }
        Map<Query, Subscription> classSubscriptions = subscriptionsByClass.get(o.className);
        if (classSubscriptions == null) {
            LOGGER.debug("no subscriptions for {}", o.className);
            return Collections.emptySet();
        }

        // Fallback to slow iteration over all other subscriptions
        for (Subscription subscription : classSubscriptions.values()) {
            if (subscription.matches(o)) {
                subscriptions.add(subscription);
            }
        }

        return subscriptions;
    }

    private static class SimpleQueryRegistry {
        // className -> field -> value -> subscription
        private final Map<String, Map<String, Map<Object, Subscription>>> subscriptions = new ConcurrentHashMap<>();

        Subscription findOrCreate(Query query) {
            Map<Object, Subscription> simpleForField = findSimpleForField(query, true);
            return simpleForField.computeIfAbsent(query.getPredicate().getValue(), o -> new Subscription(query));
        }

        void removeIfEmpty(Subscription subscription) {
            Query query = subscription.getQuery();
            Map<Object, Subscription> simpleForField = findSimpleForField(query, false);
            if (simpleForField == null) {
                return;
            }
            Map.Entry<String, Object> predicate = query.getPredicate();
            simpleForField.remove(predicate.getValue());
            if (simpleForField.isEmpty()) {
                subscriptions.get(query.className).remove(predicate.getKey());
            }
        }

        Set<Subscription> findSubscriptions(ParseMessage o) {
            Set<Subscription> subscriptions = new HashSet<>();
            Map<String, Map<Object, Subscription>> subscriptionMap = this.subscriptions.get(o.className);
            if (subscriptionMap != null) {
                for (String key : subscriptionMap.keySet()) {
                    Object objectValue = o.currentParseObject.getValue(key);
                    if (objectValue instanceof JsonObject) {
                        JsonObject jo = (JsonObject) objectValue;
                        if (POINTER.equals(jo.getString(TYPE))) {
                            objectValue = jo.getString(OBJECT_ID);
                        }
                    }
                    Subscription subscription = subscriptionMap.get(key).get(objectValue);
                    if (subscription != null) {
                        subscriptions.add(subscription);
                    }
                }
            }
            return subscriptions;
        }

        // Finds all subscriptions that listen for changes for one field (specified in the query)
        private Map<Object, Subscription> findSimpleForField(Query query, boolean create) {
            Map.Entry<String, Object> predicate = query.getPredicate();
            Map<String, Map<Object, Subscription>> classSubscriptions = subscriptions.computeIfAbsent(query.className, s -> new ConcurrentHashMap<>());
            if (create) {
                return classSubscriptions.computeIfAbsent(predicate.getKey(), s -> new ConcurrentHashMap<>());
            } else {
                return classSubscriptions.get(predicate.getKey());
            }
        }

        int count() {
            int count = 0;
            for (Map<String, Map<Object, Subscription>> fieldsMap : subscriptions.values()) {
                for (Map<Object, Subscription> fieldValuesMap : fieldsMap.values()) {
                    count += fieldValuesMap.size();
                }
            }
            return count;
        }
    }
}
