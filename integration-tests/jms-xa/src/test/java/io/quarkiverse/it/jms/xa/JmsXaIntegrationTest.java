package io.quarkiverse.it.jms.xa;

import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.common.mapper.TypeRef;

/**
 * End-to-end XA transaction tests. Verifies that JMS operations and
 * database writes participate in the same distributed transaction.
 */
@QuarkusTest
public class JmsXaIntegrationTest {

    @Test
    void jmsClientSendInXaTransaction() {
        given().body("client-send-xa")
                .post("/xa-client/send-and-insert")
                .then().statusCode(204);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<String> dbMessages = get("/xa-client/db-messages").as(new TypeRef<>() {
            });
            assertThat(dbMessages).contains("sent:client-send-xa");
        });
    }

    @Test
    void jmsClientReceiveInXaTransaction() {
        // First send a message to the queue
        given().body("client-recv-xa").post("/xa/send-to-client-queue").then().statusCode(204);

        // Then receive + insert in same XA transaction
        String received = given().post("/xa-client/receive-and-insert")
                .then().statusCode(200)
                .extract().asString();

        assertThat(received).contains("client-recv-xa");

        List<String> dbMessages = get("/xa-client/db-messages").as(new TypeRef<>() {
        });
        assertThat(dbMessages).contains("received:client-recv-xa");
    }

    @Test
    void jmsListenerXaCommitsBothJmsAndDb() {
        given().body("listener-xa").post("/xa/send-to-listener-queue").then().statusCode(204);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            List<String> messages = get("/xa/messages").as(new TypeRef<>() {
            });
            assertThat(messages).contains("listener:listener-xa");
        });
    }

    @Test
    void jmsListenerXaRollbackUndoesDbInsert() {
        given().body("listener-rollback").post("/xa/send-to-listener-rollback-queue").then().statusCode(204);

        // Wait for the listener to process (and fail)
        await().atMost(10, TimeUnit.SECONDS).pollDelay(2, TimeUnit.SECONDS).untilAsserted(() -> {
            List<String> messages = get("/xa/messages").as(new TypeRef<>() {
            });
            assertThat(messages).doesNotContain("should-not-persist-listener:listener-rollback");
        });
    }

    @Test
    void xaChainedListenersCommitAcrossBothStages() {
        given().body("chain-msg").post("/xa/send-to-chain-stage-a").then().statusCode(204);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            List<String> stageB = get("/xa/chain/stage-b").as(new TypeRef<>() {
            });
            assertThat(stageB).contains("from-a:chain-msg");
        });

        List<String> stageA = get("/xa/chain/stage-a").as(new TypeRef<>() {
        });
        assertThat(stageA).contains("chain-msg");

        List<String> dbMessages = get("/xa/messages").as(new TypeRef<>() {
        });
        assertThat(dbMessages).contains("chain-a:chain-msg", "chain-b:from-a:chain-msg");
    }

    @Test
    void xaChainedListenerRollbackPreventsDownstream() {
        given().body("chain-fail").post("/xa/send-to-chain-stage-a-fail").then().statusCode(204);

        await().atMost(10, TimeUnit.SECONDS).pollDelay(3, TimeUnit.SECONDS).untilAsserted(() -> {
            List<String> dbMessages = get("/xa/messages").as(new TypeRef<>() {
            });
            assertThat(dbMessages).doesNotContain("chain-a-fail:chain-fail");
        });

        List<String> stageB = get("/xa/chain/stage-b").as(new TypeRef<>() {
        });
        assertThat(stageB).as("Stage B should never fire because stage A rolled back the send")
                .doesNotContain("from-a-fail:chain-fail");
    }

    @Test
    void jmsClientSendXaRollbackUndoesSendAndDbInsert() {
        given().body("send-rollback-test")
                .post("/xa-client/send-and-rollback")
                .then().statusCode(500);

        List<String> dbMessages = get("/xa-client/db-messages").as(new TypeRef<>() {
        });
        assertThat(dbMessages).doesNotContain("should-not-persist-send:send-rollback-test");

        // Message should not have been sent — queue should be empty
        String received = given().post("/xa-client/receive-and-insert")
                .then().extract().asString();
        assertThat(received).doesNotContain("send-rollback-test");
    }

    @Test
    void xaListenerConcurrentProcessingWithVirtualThreads() {
        for (int i = 1; i <= 20; i++) {
            given().body("lc-" + i).post("/xa/send-to-listener-concurrent-queue").then().statusCode(204);
        }

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            List<String> processed = get("/xa/listener-concurrent/processed").as(new TypeRef<>() {
            });
            assertThat(processed).hasSize(20);
        });

        List<String> dbMessages = get("/xa/messages").as(new TypeRef<>() {
        });
        for (int i = 1; i <= 20; i++) {
            assertThat(dbMessages).contains("listener-concurrent:lc-" + i);
        }

        int maxInflight = Integer.parseInt(
                get("/xa/listener-concurrent/max-inflight").then().extract().asString());
        assertThat(maxInflight).as("Expected concurrent XA processing via @JmsListener + VT").isGreaterThan(1);
    }

    @Test
    void jmsListenerXaWithHibernateCommits() {
        given().body("hibernate-xa").post("/xa/send-to-hibernate-queue").then().statusCode(204);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            List<String> messages = get("/xa/hibernate/messages").as(new TypeRef<>() {
            });
            assertThat(messages).contains("hibernate:hibernate-xa");
        });
    }

    @Test
    void jmsListenerXaWithHibernateRollbackUndoesPersist() {
        given().body("hibernate-rollback").post("/xa/send-to-hibernate-rollback-queue").then().statusCode(204);

        await().atMost(10, TimeUnit.SECONDS).pollDelay(2, TimeUnit.SECONDS).untilAsserted(() -> {
            List<String> messages = get("/xa/hibernate/messages").as(new TypeRef<>() {
            });
            assertThat(messages).doesNotContain("should-not-persist-hibernate:hibernate-rollback");
        });
    }

    @Test
    void jmsClientXaRollbackRedeliversMessageAndUndoesDbInsert() {
        // Send a message to the rollback queue
        given().body("rollback-test")
                .post("/xa/send-to-rollback-queue")
                .then().statusCode(204);

        // Receive + DB insert + intentional failure → XA rollback
        given().post("/xa-client/receive-and-rollback")
                .then().statusCode(500);

        // Verify DB insert was rolled back
        List<String> dbMessages = get("/xa-client/db-messages").as(new TypeRef<>() {
        });
        assertThat(dbMessages).doesNotContain("should-not-persist:rollback-test");

        // Verify message was redelivered (still on the queue after rollback)
        String redelivered = given().post("/xa-client/receive-after-rollback")
                .then().statusCode(200)
                .extract().asString();

        assertThat(redelivered).contains("rollback-test");
    }

    @Test
    void jsonPojoSendAndReceive() {
        given().contentType("application/json")
                .body("{\"id\":\"ord-1\",\"product\":\"widget\",\"quantity\":5}")
                .post("/xa-client/send-pojo/pojo-queue")
                .then().statusCode(204);

        var order = get("/xa-client/receive-pojo/pojo-queue")
                .then().statusCode(200)
                .extract().as(new TypeRef<Map<String, Object>>() {
                });

        assertThat(order.get("id")).isEqualTo("ord-1");
        assertThat(order.get("product")).isEqualTo("widget");
        assertThat(order.get("quantity")).isEqualTo(5);
    }

    @Test
    void receiveWithBatchXaCommitsAllReceivesAndDbInserts() {
        for (int i = 0; i < 3; i++) {
            given().body("batch-" + i).post("/xa/send-to-batch-queue").then().statusCode(204);
        }

        int received = Integer.parseInt(
                given().post("/xa-client/receive-batch-and-insert")
                        .then().statusCode(200)
                        .extract().asString());
        assertThat(received).isEqualTo(3);

        List<String> dbMessages = get("/xa-client/db-messages").as(new TypeRef<>() {
        });
        assertThat(dbMessages).contains("batch:batch-0", "batch:batch-1", "batch:batch-2");
    }
}
