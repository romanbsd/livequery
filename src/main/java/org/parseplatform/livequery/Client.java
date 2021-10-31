package org.parseplatform.livequery;

import com.codahale.metrics.MetricRegistry;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class Client {
    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);
    private static final String CLIENT_ID = "clientId";
    private static final String OBJECT = "object";
    private final String id;
    private final ServerWebSocket ws;
    public final boolean hasMasterKey;
    //private List<String> roles;
    private final Map<Integer, SubscriptionInfo> subscriptionInfos = new HashMap<>(); // by requestId
    private final MetricRegistry metricRegistry;

    Client(ServerWebSocket ws) {
        this.id = UUID.randomUUID().toString();
        this.ws = ws;
        hasMasterKey = false;
        metricRegistry = MainVerticle.getMetricRegistry();
    }

    public String getId() {
        return id;
    }

    public void close() {
        for (Map.Entry<Integer, SubscriptionInfo> entry : subscriptionInfos.entrySet()) {
            entry.getValue().getSubscription().removeClientSubscription(this, entry.getKey());
        }
    }

    public Set<Subscription> getSubscriptions() {
        return subscriptionInfos.values().stream().
            map(SubscriptionInfo::getSubscription).collect(Collectors.toSet());
    }

    public void subscribe(Integer requestId, SubscriptionInfo info) {
        subscriptionInfos.put(requestId, info);
        info.getSubscription().addClientSubscription(this, requestId);
        pushSubscribe(requestId);
    }

    public Subscription unsubscribe(Integer requestId) {
        SubscriptionInfo subscriptionInfo = getSubscriptionInfo(requestId);
        if (subscriptionInfo == null) {
            // This can happen, as the client can send duplicate unsubscribe
            LOGGER.debug("no subscription for {}", requestId);
            pushUnsubscribe(requestId);
            return null;
        }
        Subscription subscription = subscriptionInfo.getSubscription();
        subscription.removeClientSubscription(this, requestId);
        subscriptionInfos.remove(requestId);
        pushUnsubscribe(requestId);
        return subscription;
    }

    public SubscriptionInfo getSubscriptionInfo(Integer requestId) {
        return subscriptionInfos.get(requestId);
    }

    public void pushConnect() {
        pushEvent(Event.connected, null, null);
    }

    private void pushSubscribe(Integer subscriptionId) {
        pushEvent(Event.subscribed, subscriptionId, null);
    }

    private void pushUnsubscribe(Integer subscriptionId) {
        pushEvent(Event.unsubscribed, subscriptionId, null);
    }

    public void pushEvent(Event event, Integer subscriptionId, JsonObject parseObject) {
        JsonObject response = new JsonObject();
        response.put(ParseConstants.OP, event.name());
        response.put(CLIENT_ID, id);
        if (subscriptionId != null) {
            response.put(ParseConstants.REQUEST_ID, subscriptionId);
        }

        if (parseObject != null) {
            SubscriptionInfo info;
            if (subscriptionId != null && (info = subscriptionInfos.get(subscriptionId)) != null) {
                parseObject = filterObject(parseObject, info.getFields());
            }
            response.put(OBJECT, parseObject);
        }
        ws.writeTextMessage(response.toString());
        recordMetric(event.name());
    }

    private JsonObject filterObject(JsonObject parseObject, JsonArray fields) {
        if (fields == null) {
            return parseObject;
        }
        JsonObject o = new JsonObject();
        fields.forEach(field -> {
            String key = (String) field;
            o.put(key, parseObject.getValue(key));
        });
        return o;
    }

    static JsonObject generateError(int code, String error) {
        return generateError(code, error, true);
    }

    static JsonObject generateError(int code, String error, boolean reconnect) {
        JsonObject o = new JsonObject();
        o.put(ParseConstants.OP, ParseConstants.ERROR);
        o.put(ParseConstants.ERROR, error);
        o.put("code", code);
        o.put("reconnect", reconnect);
        return o;
    }

    private void recordMetric(String op) {
        if (metricRegistry == null) {
            return;
        }
        metricRegistry.counter("parse.livequery.push." + op).inc();
    }
}
