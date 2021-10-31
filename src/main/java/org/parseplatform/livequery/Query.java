package org.parseplatform.livequery;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.parseplatform.livequery.ParseConstants.CLASS_NAME;
import static org.parseplatform.livequery.ParseConstants.OBJECT_ID;

/* Examples:
    "query":{"className":"sharedSong","where":{"user":{"__type":"Pointer","className":"_User","objectId":"Nj9CEXFvIm"}}}}
    "query":{"className":"sharedSong","where":{"$or":[{"objectId":"CqjsImPeAd"},{"objectId":"foo"}]}}}
    "query":{"className":"sharedSong","where":{"objectId":"CqjsImPeAd"}}}
 */
class Query {
    private static final Logger LOGGER = LoggerFactory.getLogger(Query.class);
    private static final String WHERE = "where";
    private static final String FIELDS = "fields";
    private static final String $OR = "$or";
    private final JsonObject where;
    private final JsonObject query;
    final JsonArray fields;
    final String className;
    private static final Map<String, Matcher> matchers = new HashMap<>();

    static {
        matchers.put("$exists", (o, key, expected) -> (Boolean) expected == o.containsKey(key));
        matchers.put("$ne", (o, key, expected) -> {
            Object value = o.getValue(key);
            return (expected == null && value != null) || (expected != null && !expected.equals(value));
        });
        matchers.put("$gt", (o, key, expected) -> {
            Number value = (Number) o.getValue(key);
            return value != null && (value.doubleValue() > ((Number) expected).doubleValue());
        });
        matchers.put("$gte", (o, key, expected) -> {
            Number value = (Number) o.getValue(key);
            return value != null && (value.doubleValue() >= ((Number) expected).doubleValue());
        });
        matchers.put("$lt", (o, key, expected) -> {
            Number value = (Number) o.getValue(key);
            return value != null && (value.doubleValue() < ((Number) expected).doubleValue());
        });
        matchers.put("$lte", (o, key, expected) -> {
            Number value = (Number) o.getValue(key);
            return value != null && (value.doubleValue() <= ((Number) expected).doubleValue());
        });
        matchers.put("$in", (o, key, expected) -> {
            Object value = o.getValue(key);
            return ((JsonArray) expected).contains(value);
        });
        matchers.put("$nin", (o, key, expected) -> {
            Object value = o.getValue(key);
            return !((JsonArray) expected).contains(value);
        });
        // TODO: regex compilation can be done once
        matchers.put("$regex", (o, key, expected) -> {
            String value = o.getString(key);
            Pattern p = Pattern.compile((String) expected);
            return p.matcher(value).find();
        });
        matchers.put("$all", (o, key, expected) -> {
            Set<Object> exp = new HashSet<Object>(((JsonArray) expected).getList());
            Set<Object> actual = new HashSet<Object>(o.getJsonArray(key).getList());
            return exp.equals(actual);
        });
    }

    interface Matcher {
        boolean matches(JsonObject o, String key, Object value);
    }

    Query(JsonObject o) {
        query = o;
        className = o.getString(ParseConstants.CLASS_NAME);
        fields = o.getJsonArray(FIELDS);
        where = o.getJsonObject(WHERE);
    }

    public boolean isSimple() {
        Object value = where.iterator().next().getValue();
        return where.size() == 1 && where.getValue($OR) == null &&
            (!(value instanceof JsonObject) || ((JsonObject) value).containsKey(CLASS_NAME));
    }

    public Map.Entry<String, Object> getPredicate() {
        if (!isSimple()) {
            return null;
        }
        Map.Entry<String, Object> predicate = where.iterator().next();
        Object value = predicate.getValue();
        if (value instanceof JsonObject) {
            value = ((JsonObject) value).getString(OBJECT_ID);
            predicate = new Entry(predicate.getKey(), value);
        }
        return predicate;
    }

    public boolean matches(JsonObject o) {
        if (o == null || !className.equals(o.getString(CLASS_NAME))) {
            return false;
        }
        if (isSimple()) {
            return simpleMatch(o);
        }

        if (where.containsKey($OR)) {
            return orMatch(o);
        } else {
            // AND match, must satisfy all conditions
            for (Map.Entry<String, Object> entry : where) {
                if (!checkCondition(o, entry)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean simpleMatch(JsonObject o) {
        Map.Entry<String, Object> entry = getPredicate();
        Object value = o.getValue(entry.getKey());
        if (value instanceof JsonObject) {
            value = ((JsonObject) value).getString(OBJECT_ID);
        }
        return entry.getValue().equals(value);
    }

    private boolean orMatch(JsonObject o) {
        JsonArray conditions = where.getJsonArray($OR);
        for (Object condition : conditions) {
            if (condition instanceof JsonObject) {
                boolean matches = true;
                for (Map.Entry<String, Object> entry : ((JsonObject) condition)) {
                    matches = checkCondition(o, entry);
                    if (!matches) {
                        break;
                    }
                }
                if (matches) {
                    return true;
                }
            } else {
                LOGGER.warn("Unexpected condition: {}", condition);
            }
        }
        return false;
    }

    private boolean checkCondition(JsonObject o, Map.Entry<String, Object> entry) {
        String key = entry.getKey();
        Object expected = entry.getValue();
        if (expected instanceof JsonObject) {
            Map.Entry<String, Object> opAndVal = ((JsonObject) expected).iterator().next();
            String op = opAndVal.getKey();
            if (!matchers.containsKey(op)) {
                LOGGER.warn("Unsupported operator {}", op);
                return false;
            }
            return matchers.get(op).matches(o, key, opAndVal.getValue());
        } else {
            Object value = o.getValue(key);
            return (expected == null && value == null) || (expected != null && expected.equals(value));
        }
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
        if (!(obj instanceof Query)) {
            return false;
        }
        return query.equals(((Query) obj).query);
    }

    @Override
    public String toString() {
        return query.toString();
    }

    static final class Entry implements Map.Entry<String, Object> {
        final String key;
        final Object value;

        public Entry(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public Object setValue(Object value) {
            throw new UnsupportedOperationException();
        }
    }
}
