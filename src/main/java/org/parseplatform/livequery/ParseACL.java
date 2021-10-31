package org.parseplatform.livequery;

import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

class ParseACL {
    private static final String DEFAULT_ACL = "*";
    private static final String READ = "read";
    private static final String WRITE = "write";
    private final Map<String, Permission> permissions = new HashMap<>();

    public ParseACL(JsonObject o) {
        //noinspection unchecked
        o.getMap().forEach((key, val) -> permissions.put(key, new Permission((Map<String, Boolean>) val)));
    }

    public boolean isReadableBy(String userId) {
        return isPubliclyReadable() || (permissions.containsKey(userId) && permissions.get(userId).read);
    }

    public boolean isPubliclyReadable() {
        return permissions.get(DEFAULT_ACL).read;
    }

    private static class Permission {
        final boolean read;
        final boolean write;

        Permission(Map<String, Boolean> val) {
            read = val.getOrDefault(READ, false);
            write = val.getOrDefault(WRITE, false);
        }
    }
}
