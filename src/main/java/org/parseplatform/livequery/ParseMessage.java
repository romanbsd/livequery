package org.parseplatform.livequery;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

public class ParseMessage {
    private static final String CURRENT_PARSE_OBJECT = "currentParseObject";
    private static final String ORIGINAL_PARSE_OBJECT = "originalParseObject";
    private static final String VALUE = "value";
    private static final String MESSAGE = "message";

    public final String className;
    public final String objectId;
    public final JsonObject currentParseObject;
    public final JsonObject originalParseObject;
    private final ParseACL acl;

    static ParseMessage create(Message<JsonObject> message) {
        JsonObject o = new JsonObject(message.body().getJsonObject(VALUE).getString(MESSAGE));
        return new ParseMessage(o);
    }

    ParseMessage(JsonObject o) {
        currentParseObject = o.getJsonObject(CURRENT_PARSE_OBJECT);
        className = currentParseObject.getString(ParseConstants.CLASS_NAME);
        objectId = currentParseObject.getString(ParseConstants.OBJECT_ID);
        originalParseObject = o.getJsonObject(ORIGINAL_PARSE_OBJECT);
        o = currentParseObject.getJsonObject(ParseConstants.ACL);
        acl = (o == null) ? null : new ParseACL(o);
    }

    public boolean isReadableBy(String userId) {
        return acl == null || acl.isReadableBy(userId);
    }

    public boolean isPubliclyReadable() {
        return acl == null || acl.isPubliclyReadable();
    }
}
