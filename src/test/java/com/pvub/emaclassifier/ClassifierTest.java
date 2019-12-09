package com.pvub.emaclassifier;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.net.ServerSocket;
import org.junit.*;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * API Test
 * @author Udai
 */
@RunWith(VertxUnitRunner.class)
public class ClassifierTest {
    private Logger m_logger;
    private Vertx vertx;
    private Integer port;

  @BeforeClass
  public static void initialize() throws IOException {
      System.setProperty("log4j.configurationFile","log4j2-test.xml");
  }

  @AfterClass
  public static void shutdown() {
  }

  @Before
  public void setUp(TestContext context) throws IOException {
    m_logger = LoggerFactory.getLogger("TEST");
    vertx = Vertx.vertx();

    // Let's configure the verticle to listen on the 'test' port (randomly picked).
    // We create deployment options and set the _configuration_ json object:
    ServerSocket socket = new ServerSocket(0);
    port = socket.getLocalPort();
    socket.close();

    DeploymentOptions options = new DeploymentOptions()
        .setConfig(new JsonObject()
            .put("port", port)
        );

    m_logger.info("Deploying at port {}", port);
    // We pass the options as the second parameter of the deployVerticle method.
    vertx.deployVerticle(EMAClassifierVerticle.class.getName(), options, context.asyncAssertSuccess());
  }

  @After
  public void tearDown(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }
  
  @Test
  public void checkUnique(TestContext context) {
    Async async = context.async();
    JsonObject obj = new JsonObject();
    JsonArray  emails = new JsonArray();
    emails.add("first.last@mail.com");
    emails.add("last.first@mail.com");
    emails.add("first.last+test@mail.com");
    emails.add("first.last+123@mail.com");
    emails.add("first.last.test@mail.com");
    emails.add("first.last.123@mail.com");
    emails.add("first.last@snail.com");
    obj.put("emails", emails);
    final String json = Json.encode(obj);
    m_logger.info("Verify call {}", json);
    vertx.createHttpClient().post(port, "localhost", "/api/verify")
        .putHeader("content-type", "application/json")
        .putHeader("content-length", Integer.toString(json.length()))
        .handler(response -> {
          context.assertEquals(response.statusCode(), 200);
          context.assertTrue(response.headers().get("content-type").contains("application/json"));
          response.bodyHandler(body -> {
            JsonObject resp = new JsonObject(body.toString());
            context.assertEquals(resp.getInteger("unique-count"), 5);
            async.complete();
          });
        })
        .write(json)
        .end();
    async.await();
  }  
}
