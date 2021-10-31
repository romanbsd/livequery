package org.parseplatform.livequery;

import io.vertx.core.Vertx;

public class LiveQueryServer {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new MainVerticle());
    }
}
