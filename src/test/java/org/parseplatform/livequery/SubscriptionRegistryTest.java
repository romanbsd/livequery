package org.parseplatform.livequery;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SubscriptionRegistryTest {
    private SubscriptionRegistry registry;
    private Query query;

    @BeforeEach
    void init() {
        registry = SubscriptionRegistry.getInstance();
        JsonObject o = new JsonObject("{\"className\":\"sharedSong\",\"where\":{\"objectId\":\"CqjsImPeAd\"}}");
        query = new Query(o);
    }

    @Test
    void findOrCreate() {
        Subscription subscription = registry.findOrCreate(query);
        assertNotNull(subscription);

        Subscription found = registry.findOrCreate(query);
        assertSame(subscription, found);
    }

    @Test
    void findSubscriptions() {
        Subscription subscription = registry.findOrCreate(query);
        JsonObject o = new JsonObject("{\"currentParseObject\":{\"objectId\":\"CqjsImPeAd\", \"__type\":\"Object\", \"className\":\"sharedSong\"}, \"originalParseObject\":null}");
        ParseMessage message = new ParseMessage(o);
        Set<Subscription> subscriptions = registry.findSubscriptions(message);
        assertAll("found",
            () -> assertEquals(1, subscriptions.size()),
            () -> assertSame(subscription, subscriptions.toArray()[0]));
    }

    @Test
    void findByObject() {
        Query query = new Query(new JsonObject("{\"className\":\"sharedSong\",\"where\":{\"user\":{\"__type\":\"Pointer\",\"className\":\"_User\",\"objectId\":\"Va4M39CvxD\"}}}"));
        Subscription subscription = registry.findOrCreate(query);
        JsonObject o = new JsonObject("{\"currentParseObject\":{\"user\":{\"__type\":\"Pointer\",\"className\":\"_User\",\"objectId\":\"Va4M39CvxD\"},\"objectId\":\"yHOgbVKPFA\",\"__type\":\"Object\",\"className\":\"sharedSong\"}}");
        ParseMessage message = new ParseMessage(o);
        Set<Subscription> subscriptions = registry.findSubscriptions(message);
        assertAll("found",
            () -> assertEquals(1, subscriptions.size()),
            () -> assertSame(subscription, subscriptions.toArray()[0]));
    }

    @Test
    void cleanup() {
        Subscription subscription1 = registry.findOrCreate(query);
        JsonObject o = new JsonObject("{\"className\":\"sharedSong\",\"where\":{\"user\":{\"__type\":\"Pointer\",\"className\":\"_User\",\"objectId\":\"Nj9CEXFvIm\"}}}");
        Query query2 = new Query(o);
        Subscription subscription2 = registry.findOrCreate(query2);

        o = new JsonObject("{\"className\":\"sharedSong\",\"where\":{\"$or\":[{\"objectId\":\"CqjsImPeAd\"},{\"objectId\":\"foo\"}]}}");
        Query query3 = new Query(o);
        Subscription subscription3 = registry.findOrCreate(query3);

        assertEquals(3, registry.count());

        registry.removeIfEmpty(subscription1);
        registry.removeIfEmpty(subscription2);
        registry.removeIfEmpty(subscription3);

        assertEquals(0, registry.count());
    }

}
