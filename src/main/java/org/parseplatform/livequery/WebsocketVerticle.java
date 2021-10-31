package org.parseplatform.livequery;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class WebsocketVerticle extends AbstractVerticle {
    private static final Logger LOGGER = LoggerFactory.getLogger(WebsocketVerticle.class);
    private static final String SESSION_TOKEN = "sessionToken";
    private final SubscriptionRegistry subscriptionRegistry = SubscriptionRegistry.getInstance();
    private final Map<ServerWebSocket, Client> socketToClient = new HashMap<>();
    private MetricRegistry metricRegistry;

    enum Metric {
        subscribe,
        unsubscribe,
        connect,
        disconnect
    }

    @Override
    public void start() {
        int port = config().getInteger(ConfigKey.PORT, 3000);
        vertx.createHttpServer().webSocketHandler((ServerWebSocket ws) -> {
            LOGGER.debug("onConnect ws {}", ws.textHandlerID());
            if (ws.path().equals("/parse")) {
                ws.handler(buffer -> onWsData(ws, buffer));
                ws.closeHandler((v) -> onClose(ws));
            } else {
                ws.reject();
            }
        }).requestHandler((handler) -> handler.response().end()).listen(port);
        LOGGER.info("Listening on port {}", port);

        if (vertx.isMetricsEnabled()) {
            LOGGER.info("Metrics enabled");
            metricRegistry = MainVerticle.getMetricRegistry();
            metricRegistry.register("parse.livequery.clients", (Gauge<Integer>) socketToClient::size);
        }
    }

    private void onClose(ServerWebSocket ws) {
        Client client = socketToClient.get(ws);
        if (client == null) {
            LOGGER.info("onClose: Cannot find client for ws: {}", ws.textHandlerID());
            return;
        }
        client.close();
        for (Subscription s : client.getSubscriptions()) {
            subscriptionRegistry.removeIfEmpty(s);
        }
        socketToClient.remove(ws);
        LOGGER.debug("Removed client {} ws: {}", client.getId(), ws.textHandlerID());
        LOGGER.debug("Current client number: {}", socketToClient.size());
        LOGGER.debug("Subscriptions count: {}", subscriptionRegistry.count());
        recordMetric(Metric.disconnect);
    }

    private void onWsData(ServerWebSocket ws, Buffer buffer) {
        JsonObject o;
        try {
            o = buffer.toJsonObject();
        } catch (DecodeException e) {
            LOGGER.error("onWsData", e);
            sendError(ws, 1, e.getMessage());
            return;
        }
        String op = o.getString(ParseConstants.OP);
        switch (op) {
            case "connect":
                handleConnect(ws, o);
                break;
            case "subscribe":
                handleSubscribe(ws, o);
                break;
            case "update":
                handleUpdate(ws, o);
                break;
            case "unsubscribe":
                handleUnsubscribe(ws, o);
                break;
            default:
                sendError(ws, 3, "Unknown operation " + op);
        }
    }

    // {"op":"unsubscribe","requestId":1}
    private void handleUnsubscribe(ServerWebSocket ws, JsonObject o) {
        Client client = getClient(ws);
        if (client == null) {
            return;
        }
        Integer requestId = o.getInteger(ParseConstants.REQUEST_ID);
        LOGGER.debug("Unsubcribing client {} from subscription {}", client.getId(), requestId);
        Subscription subscription = client.unsubscribe(requestId);
        subscriptionRegistry.removeIfEmpty(subscription);
        recordMetric(Metric.unsubscribe);
    }

    @SuppressWarnings("unused")
    private void handleUpdate(ServerWebSocket ws, JsonObject o) {
        LOGGER.warn("update is unsupported");
    }

    // {"op":"subscribe","requestId":1,"query":{"className":"sharedSong","where":{"objectId":"CqjsImPeAd"}}}
    private void handleSubscribe(ServerWebSocket ws, JsonObject o) {
        Client client = getClient(ws);
        if (client == null) {
            return;
        }
        Query query = new Query(o.getJsonObject(ParseConstants.QUERY));
        Subscription subscription = subscriptionRegistry.findOrCreate(query);

        SubscriptionInfo subscriptionInfo = new SubscriptionInfo(subscription);
        subscriptionInfo.setSessionToken(o.getString(SESSION_TOKEN));

        Integer requestId = o.getInteger(ParseConstants.REQUEST_ID);
        client.subscribe(requestId, subscriptionInfo);

        LOGGER.debug("Subscribed client {} to subscription {}: {}", client.getId(), requestId, query);
        LOGGER.debug("Current client number: {}", socketToClient.size());
        recordMetric(Metric.subscribe);
    }

    private Client getClient(ServerWebSocket ws) {
        Client client = socketToClient.get(ws);
        if (client == null) {
            sendError(ws, 2, "Can't find this client");
            return null;
        }
        return client;
    }

    // {"op":"connect","applicationId":"xyz"}
    private void handleConnect(ServerWebSocket ws, JsonObject o) {
        String appId = o.getString(ConfigKey.APP_ID);
        if (!config().getString(ConfigKey.APP_ID).equals(appId)) {
            LOGGER.warn("Invalid applicationId: {}", appId);
            return;
        }
        //TODO: check keyPairs
        Client client = new Client(ws);
        socketToClient.put(ws, client);
        String id = client.getId();
        LOGGER.info("Created new client: {} ws: {}", id, ws.textHandlerID());
        client.pushConnect();
        recordMetric(Metric.connect);
    }

    private void recordMetric(Metric event) {
        if (metricRegistry == null) {
            return;
        }
        metricRegistry.counter("parse.livequery.event." + event).inc();
    }

    private void sendError(ServerWebSocket ws, int code, String error) {
        LOGGER.warn("{}; ws: {}", error, ws.textHandlerID());
        ws.writeTextMessage(Client.generateError(code, error).toString());
    }
}
