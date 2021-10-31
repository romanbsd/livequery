package org.parseplatform.livequery;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.ext.web.impl.LRUCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UsersVerticle extends AbstractVerticle {
    private static final Logger LOGGER = LoggerFactory.getLogger(UsersVerticle.class);
    private static final int CACHE_SIZE = 10000;
    private static final String X_PARSE_APPLICATION_ID = "X-Parse-Application-Id";
    private static final String X_PARSE_MASTER_KEY = "X-Parse-Master-Key";
    private static final String USER = "user";
    private static final String RESULTS = "results";
    private final LRUCache<String, String> userIds = new LRUCache<>(CACHE_SIZE);
    private WebClient webClient;
    private String uri;

    @Override
    public void start() {
        webClient = WebClient.create(vertx);
        uri = config().getString(ConfigKey.SERVER_URL) + "/classes/_Session";
        vertx.eventBus().consumer(getClass().getName(), this::onRequest);
    }

    private void onRequest(Message<String> message) {
        final String token = message.body();
        String userId = userIds.get(token);
        if (userId != null) {
            message.reply(userId);
        } else {
            JsonObject query = buildQuery(token);
            webClient.getAbs(uri).
                putHeader(X_PARSE_APPLICATION_ID, config().getString(ConfigKey.APP_ID)).
                putHeader(X_PARSE_MASTER_KEY, config().getString(ConfigKey.MASTER_KEY)).
                as(BodyCodec.jsonObject()).
                sendJsonObject(query, ar -> {
                    if (ar.succeeded()) {
                        HttpResponse<JsonObject> response = ar.result();
                        if (response.statusCode() == 200) {
                            JsonArray results = response.body().getJsonArray(RESULTS);
                            if (results.isEmpty()) {
                                message.fail(404, "Session not found");
                            } else {
                                String uid = results.getJsonObject(0).getJsonObject(USER).getString(ParseConstants.OBJECT_ID);
                                userIds.put(token, uid);
                                message.reply(uid);
                            }
                        } else {
                            LOGGER.warn("parse server returned {}", response.statusCode());
                            message.fail(response.statusCode(), response.statusMessage());
                        }
                    } else {
                        LOGGER.warn("failed to retrieve userId", ar.cause());
                        message.reply(ar.cause());
                    }
                });
        }
    }

    private JsonObject buildQuery(String token) {
        return new JsonObject().
            put("where", new JsonObject().put("sessionToken", token)).
            put("limit", 1);
    }
}
