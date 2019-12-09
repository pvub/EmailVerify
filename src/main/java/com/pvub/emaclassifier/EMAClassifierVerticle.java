package com.pvub.emaclassifier;

import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.http.HttpServerResponse;
import io.vertx.rxjava.ext.web.Router;
import io.vertx.rxjava.ext.web.RoutingContext;
import io.vertx.rxjava.ext.web.handler.BodyHandler;
import io.vertx.rxjava.ext.web.handler.CorsHandler;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

/**
 * Verticle to handle API requests
 * @author Udai
 */
public class EMAClassifierVerticle extends AbstractVerticle {
    static final int WORKER_POOL_SIZE = 20;

    private final Logger    m_logger;
    private Router          m_router;
    private JsonObject      m_config = null;
    
    public EMAClassifierVerticle() {
        super();
        m_logger = LoggerFactory.getLogger("APIVERTICLE");
    }
    @Override
    public void start() throws Exception {
        m_config = config();
        m_logger.info("Starting EMAClassifierVerticle");

        // Create a router object.
        m_router = Router.router(vertx);
        // add a handler which sets the request body on the RoutingContext.
        m_router.route().handler(BodyHandler.create());

        // Handle CORS requests.
        m_router.route().handler(CorsHandler.create("*")
            .allowedMethod(HttpMethod.GET)
            .allowedMethod(HttpMethod.OPTIONS)
            .allowedHeader("Accept")
            .allowedHeader("Authorization")
            .allowedHeader("Content-Type"));

        m_router.get("/health").handler(this::generateHealth);
        m_router.post("/api/verify").handler(this::verifyEmails);

        int port = m_config.getInteger("port", 8080);
        // Create the HTTP server and pass the 
        // "accept" method to the request handler.
        vertx
            .createHttpServer()
            .requestHandler(m_router::accept)
            .listen(
                // Retrieve the port from the 
                // configuration, default to 8080.
                port,
                result -> {
                    if (result.succeeded()) {
                        m_logger.info("Listening now on port {}", port);
                    } else {
                        m_logger.error("Failed to listen", result.cause());
                    }
                }
            );
    }
    
    @Override
    public void stop() throws Exception {
        m_logger.info("Stopping StarterVerticle");
    }
    
    private void verifyEmails(RoutingContext rc) {
        HttpServerResponse response = rc.response();
        String emailsJson = rc.getBodyAsString();
        JsonObject obj = new JsonObject(emailsJson);
        JsonArray emails = obj.getJsonArray("emails");
        HashSet<String> uniqueEmails = new HashSet<String>();
        Observable.from(emails)
                  .map(email -> strip((String)email))
                  .filter(strippedemail -> {
                      return uniqueEmails.add(strippedemail);
                  })
                  .count()
                  .subscribe(count -> {
                                respond(response, count);
                            }
                            , e -> {}
                            , () -> {});
    }
    
    private String strip(String email) {
        String[] parts = email.split("@");
        if (parts.length == 2) {
            StringBuilder stripped = new StringBuilder();
            int len = parts[0].length();
            for (int index = 0; index < len; ++index) {
                final char ch = parts[0].charAt(index);
                if (ch == '.') {
                    continue;
                }
                if (ch == '+') {
                    break;
                }
                if (ch == '@') {
                    break;
                }
                stripped.append(ch);
            }
            stripped.append(parts[1]);
            return stripped.toString();
        } else {
            return "";
        }
    }
    
    private void respond(HttpServerResponse response, int count) {
        JsonObject obj = new JsonObject();
        obj.put("unique-count", count);
        response.setStatusCode(200)
              .putHeader("content-type", "application/json; charset=utf-8")
              .end(Json.encodePrettily(obj));
    }
    
    public void generateHealth(RoutingContext ctx) {
        ctx.response()
            .setChunked(true)
            .putHeader("Content-Type", "application/json;charset=UTF-8")
            .putHeader("Access-Control-Allow-Methods", "GET")
            .putHeader("Access-Control-Allow-Origin", "*")
            .putHeader("Access-Control-Allow-Headers", "Accept, Authorization, Content-Type")
            .write((new JsonObject().put("status", "OK")).encode())
            .end();
    }

}