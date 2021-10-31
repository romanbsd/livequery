package org.parseplatform.livequery;

import io.vertx.core.json.JsonArray;

class SubscriptionInfo {
    private final Subscription subscription;
    private String sessionToken;

    public SubscriptionInfo(Subscription subscription) {
        this.subscription = subscription;
    }

    public Subscription getSubscription() {
        return subscription;
    }

    public JsonArray getFields() {
        return subscription.getQuery().fields;
    }

    public String getSessionToken() {
        return sessionToken;
    }

    public void setSessionToken(String sessionToken) {
        this.sessionToken = sessionToken;
    }
}
