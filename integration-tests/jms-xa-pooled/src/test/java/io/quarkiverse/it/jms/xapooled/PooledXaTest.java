package io.quarkiverse.it.jms.xapooled;

import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.common.mapper.TypeRef;

@QuarkusTest
public class PooledXaTest {

    @Test
    void sendCommitsBothJmsAndDb() {
        given().body("commit-test")
                .post("/pooled-xa/send-and-insert")
                .then().statusCode(204);

        List<String> dbMessages = get("/pooled-xa/db-messages").as(new TypeRef<>() {
        });
        assertThat(dbMessages).contains("pooled-sent:commit-test");

        String received = given().post("/pooled-xa/receive-and-insert")
                .then().statusCode(200)
                .extract().asString();
        assertThat(received).contains("commit-test");

        dbMessages = get("/pooled-xa/db-messages").as(new TypeRef<>() {
        });
        assertThat(dbMessages).contains("pooled-received:commit-test");
    }

    @Test
    void rollbackUndoesBothSendAndDb() {
        given().body("rollback-test")
                .post("/pooled-xa/send-and-rollback")
                .then().statusCode(500);

        List<String> dbMessages = get("/pooled-xa/db-messages").as(new TypeRef<>() {
        });
        assertThat(dbMessages).doesNotContain("pooled-should-not-persist:rollback-test");

        String received = given().post("/pooled-xa/receive-and-insert")
                .then().extract().asString();
        assertThat(received).doesNotContain("rollback-test");
    }
}
