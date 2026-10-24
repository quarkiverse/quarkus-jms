package io.quarkiverse.it.jms.nativepojo;

import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class NativePojoTest {

    @Test
    void pojoPayloadRoundTrip() {
        given().body("ord-1,widget,5").post("/orders/send").then().statusCode(204);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            String received = get("/orders/received").then().statusCode(200).extract().asString();
            assertThat(received).contains("ord-1,widget,5");
        });
    }
}
