package org.parseplatform.livequery;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class QueryTest {
    private Query queryById;
    private Query queryByObject;
    private Query queryWithOr;
    private Query queryWithOp;

    private Query buildQuery(String str) {
        return new Query(new JsonObject(str));
    }

    @BeforeEach
    void beforeEach() {
        queryById = buildQuery("{\"className\":\"Song\",\"where\":{\"objectId\":\"CqjsImPeAd\"}}");
        queryByObject = buildQuery("{\"className\":\"Song\",\"where\":{\"user\":{\"__type\":\"Pointer\",\"className\":\"_User\",\"objectId\":\"Nj9CEXFvIm\"}}}");
        queryWithOr = buildQuery("{\"className\":\"Song\",\"where\":{\"$or\":[{\"objectId\":\"CqjsImPeAd\"},{\"objectId\":\"foo\"}]}}");
        queryWithOp = buildQuery("{\"className\":\"_User\",\"where\":{\"age\":{\"$gt\":18}}}");
    }

    @Test
    void isSimple() {
        assertTrue(queryById.isSimple());
        assertTrue(queryByObject.isSimple());
        assertFalse(queryWithOr.isSimple());
        assertFalse(queryWithOp.isSimple());
    }

    @Test
    void getPredicate() {
        Map.Entry<String, Object> predicate = queryById.getPredicate();
        assertEquals(predicate.getKey(), "objectId");
        assertEquals(predicate.getValue(), "CqjsImPeAd");

        predicate = queryByObject.getPredicate();
        assertEquals(predicate.getKey(), "user");
        assertEquals(predicate.getValue(), "Nj9CEXFvIm");

        predicate = queryWithOr.getPredicate();
        assertNull(predicate);
    }

    @Test
    void matches() {
        JsonObject o = new JsonObject("{\"objectId\":\"CqjsImPeAd\", \"__type\":\"Object\", \"className\":\"Song\"}");
        assertTrue(queryById.matches(o));
        assertFalse(queryByObject.matches(o));
        assertTrue(queryWithOr.matches(o));

        o = new JsonObject("{\"user\":{\"__type\":\"Pointer\", \"className\":\"_User\", \"objectId\":\"Nj9CEXFvIm\"},\"objectId\":\"AAjsImPeAd\", \"__type\":\"Object\", \"className\":\"Song\"}");
        assertFalse(queryById.matches(o));
        assertTrue(queryByObject.matches(o));
        assertFalse(queryWithOr.matches(o));

        o = new JsonObject("{\"objectId\":\"Nj9CEXFvIm\", \"__type\":\"Object\", \"className\":\"_User\"}");
        assertFalse(queryById.matches(o));
        assertFalse(queryByObject.matches(o));
        assertFalse(queryWithOr.matches(o));

        o = new JsonObject("{\"objectId\":\"CqjsImPeAd\", \"__type\":\"Object\", \"className\":\"Another\"}");
        assertFalse(queryById.matches(o));
        assertFalse(queryByObject.matches(o));
        assertFalse(queryWithOr.matches(o));
    }

    @Test
    void exists() {
        Query q1 = buildQuery("{\"className\":\"_User\",\"where\":{\"name\":{\"$exists\":false}}}");
        Query q2 = buildQuery("{\"className\":\"_User\",\"where\":{\"name\":{\"$exists\":true}}}");
        JsonObject o1 = new JsonObject("{\"objectId\":\"Nj9CEXFvIm\", \"__type\":\"Object\", \"className\":\"_User\", \"name\":null}");
        JsonObject o2 = new JsonObject("{\"objectId\":\"Nj9CEXFvIm\", \"__type\":\"Object\", \"className\":\"_User\"}");
        assertFalse(q1.matches(o1));
        assertTrue(q1.matches(o2));
        assertTrue(q2.matches(o1));
        assertFalse(q2.matches(o2));
    }

    @Test
    void notEquals() {
        Query q = buildQuery("{\"className\":\"_User\",\"where\":{\"age\":{\"$ne\":18}}}");
        JsonObject o1 = new JsonObject("{\"objectId\":\"Nj9CEXFvIm\", \"__type\":\"Object\", \"className\":\"_User\", \"age\":18}");
        JsonObject o2 = new JsonObject("{\"objectId\":\"Nj9CEXFvIm\", \"__type\":\"Object\", \"className\":\"_User\", \"age\":19}");
        assertFalse(q.matches(o1));
        assertTrue(q.matches(o2));

        q = buildQuery("{\"className\":\"_User\",\"where\":{\"name\":{\"$ne\":null}}}");
        o1 = new JsonObject("{\"objectId\":\"Nj9CEXFvIm\", \"__type\":\"Object\", \"className\":\"_User\", \"name\":null}");
        o2 = new JsonObject("{\"objectId\":\"Nj9CEXFvIm\", \"__type\":\"Object\", \"className\":\"_User\", \"name\":\"User\"}");
        assertFalse(q.matches(o1));
        assertTrue(q.matches(o2));
    }

    @Test
    void greaterThan() {
        Query q = buildQuery("{\"className\":\"_User\",\"where\":{\"age\":{\"$gt\":18}}}");
        JsonObject o1 = new JsonObject("{\"objectId\":\"Nj9CEXFvIm\", \"__type\":\"Object\", \"className\":\"_User\", \"age\":17}");
        JsonObject o2 = new JsonObject("{\"objectId\":\"Nj9CEXFvIm\", \"__type\":\"Object\", \"className\":\"_User\", \"age\":18}");
        JsonObject o3 = new JsonObject("{\"objectId\":\"Nj9CEXFvIm\", \"__type\":\"Object\", \"className\":\"_User\", \"age\":19}");
        assertFalse(q.matches(o1));
        assertFalse(q.matches(o2));
        assertTrue(q.matches(o3));
    }

    @Test
    void lessThan() {
        Query q = buildQuery("{\"className\":\"_User\",\"where\":{\"age\":{\"$lt\":18}}}");
        JsonObject o1 = new JsonObject("{\"objectId\":\"Nj9CEXFvIm\", \"__type\":\"Object\", \"className\":\"_User\", \"age\":17}");
        JsonObject o2 = new JsonObject("{\"objectId\":\"Nj9CEXFvIm\", \"__type\":\"Object\", \"className\":\"_User\", \"age\":18}");
        JsonObject o3 = new JsonObject("{\"objectId\":\"Nj9CEXFvIm\", \"__type\":\"Object\", \"className\":\"_User\", \"age\":19}");
        assertTrue(q.matches(o1));
        assertFalse(q.matches(o2));
        assertFalse(q.matches(o3));
    }

    @Test
    void containedIn() {
        Query q = buildQuery("{\"className\":\"_User\",\"where\":{\"country\":{\"$in\":[\"UK\",\"US\"]}}}");
        JsonObject o1 = new JsonObject("{\"objectId\":\"Nj9CEXFvIm\", \"__type\":\"Object\", \"className\":\"_User\",\"country\":\"UK\"}");
        JsonObject o2 = new JsonObject("{\"objectId\":\"Nj9CEXFvIm\", \"__type\":\"Object\", \"className\":\"_User\",\"country\":\"France\"}");
        assertTrue(q.matches(o1));
        assertFalse(q.matches(o2));
    }

    @Test
    void notContainedIn() {
        Query q = buildQuery("{\"className\":\"_User\",\"where\":{\"country\":{\"$nin\":[\"UK\",\"US\"]}}}");
        JsonObject o = new JsonObject("{\"objectId\":\"Nj9CEXFvIm\", \"__type\":\"Object\", \"className\":\"_User\",\"country\":\"France\"}");
        assertTrue(q.matches(o));
    }

    @Test
    void containsAll() {
        Query q = buildQuery("{\"className\":\"_User\",\"where\":{\"languages\":{\"$all\":[\"English\",\"Spanish\"]}}}");
        JsonObject o1 = new JsonObject("{\"objectId\":\"Nj9CEXFvIm\", \"__type\":\"Object\", \"className\":\"_User\",\"languages\":[\"English\"]}");
        JsonObject o2 = new JsonObject("{\"objectId\":\"Nj9CEXFvIm\", \"__type\":\"Object\", \"className\":\"_User\",\"languages\":[\"Spanish\",\"English\"]}");
        assertFalse(q.matches(o1));
        assertTrue(q.matches(o2));
    }

    @Test
    void contains() {
        Query q = buildQuery("{\"className\":\"_User\",\"where\":{\"name\":{\"$regex\":\"\\\\Qoma\\\\E\"}}}");
        JsonObject o1 = new JsonObject("{\"objectId\":\"Nj9CEXFvIm\", \"__type\":\"Object\", \"className\":\"_User\",\"name\":\"Roman\"}");
        JsonObject o2 = new JsonObject("{\"objectId\":\"Nj9CEXFvIm\", \"__type\":\"Object\", \"className\":\"_User\",\"name\":\"Ron\"}");
        assertTrue(q.matches(o1));
        assertFalse(q.matches(o2));
    }
}
