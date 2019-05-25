package DAO.messages

import consts.*
import consts.messages.MessageParams
import io.vertx.core.DeploymentOptions
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.rxjava.core.Vertx
import org.junit.Rule
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.extension.ExtendWith
import org.testcontainers.containers.FixedHostPortGenericContainer
import org.testcontainers.containers.output.OutputFrame
import org.testcontainers.containers.output.WaitingConsumer

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension::class)
class MessageDaoVerticleTest {

    @Rule
    public val arangoContainer = FixedHostPortGenericContainer<Nothing>("arangodb/arangodb:3.4.4")


    @BeforeAll
    fun setUp(vertx: Vertx, context: VertxTestContext) {
        arangoContainer.withEnv(mutableMapOf("ARANGO_NO_AUTH" to "0"))
        arangoContainer.withFixedExposedPort(8529, 8529)
        arangoContainer.start()
        val consumer = WaitingConsumer()
        arangoContainer.followOutput(consumer, OutputFrame.OutputType.STDOUT)
        consumer.waitUntil {
            it.utf8String.contains("ArangoDB (version 3.4.4 [linux]) is ready for business. Have fun!")
        }

        val config = prepareConfig()

        val options = DeploymentOptions().setConfig(config)

        vertx.deployVerticle(MessageDaoVerticle(), options, context.completing())
    }

    /**
     * Prepare config for current test
     */
    private fun prepareConfig(): JsonObject {
        val json = JsonObject()
        json.put(ConfigParameters.ArangoDbHost.name, "localhost")
        json.put(ConfigParameters.ArangoDbPort.name, 8529)
        return json
    }

    @Test
    fun testCreateMessage(vertx: Vertx, context: VertxTestContext) {
        val messageToVerticle = JsonObject().apply {
            put(FieldLabels.DaoMethod.name, DAOMethods.CREATE.name)
            put(MessageParams.USER_ID.text, "kek")
        }

        vertx.eventBus().rxSend<JsonObject>(EventBusAddresses.MessageDao.name, messageToVerticle)
                .subscribe({
                    assertNotNull(it.body().getString(MessageParams.KEY.text))
                    context.completeNow()
                }, {
                    context.failNow(it)
                })
    }

    @Test
    fun testGetMessage(vertx: Vertx, context: VertxTestContext) {
        val messageToVerticle = JsonObject().apply {
            put(FieldLabels.DaoMethod.name, DAOMethods.CREATE.name)
            put("test", "param")
        }

        vertx.eventBus().rxSend<JsonObject>(EventBusAddresses.MessageDao.name, messageToVerticle).flatMap {
            vertx.eventBus().rxSend<JsonObject>(EventBusAddresses.MessageDao.name, JsonObject().apply {
                put(FieldLabels.DaoMethod.name, DAOMethods.GET.name)
                put(MessageParams.KEY.text, it.body().getString(MessageParams.KEY.text))
            })
        }.subscribe({
            val body = it.body()
            assertNotNull(body)
            assertNotNull(body.getString(MessageParams.KEY.text))
            assertEquals(body.getString("test"), "param")
            context.completeNow()
        }, {
            context.failNow(it)
        })
    }

    @Test
    fun testDeleteMessage(vertx: Vertx, context: VertxTestContext) {
        val message = JsonObject().apply {
            put(FieldLabels.DaoMethod.name, DAOMethods.CREATE.name)
        }

        vertx.eventBus().rxSend<JsonObject>(EventBusAddresses.MessageDao.name, message).flatMap {
            vertx.eventBus().rxSend<JsonObject>(EventBusAddresses.MessageDao.name, JsonObject().apply {
                put(FieldLabels.DaoMethod.name, DAOMethods.DELETE.name)
                put(MessageParams.KEY.text, it.body().getString(MessageParams.KEY.text))
            })
        }.flatMap {
            vertx.eventBus().rxSend<JsonObject>(EventBusAddresses.MessageDao.name, JsonObject().apply {
                put(FieldLabels.DaoMethod.name, DAOMethods.GET.name)
                put(MessageParams.KEY.text, it.body().getString(MessageParams.KEY.text))
            })
        }.subscribe({
            assertNull(it.body())
            context.completeNow()
        }, {
            context.failNow(it)
        })
    }

    @Test
    fun testUpdateMessage(vertx: Vertx, context: VertxTestContext) {
        val message = JsonObject().apply {
            put(FieldLabels.DaoMethod.name, DAOMethods.CREATE.name)
            put("param", "test")
            put("user_id", "kek")
        }

        vertx.eventBus().rxSend<JsonObject>(EventBusAddresses.MessageDao.name, message).flatMap {
            vertx.eventBus().rxSend<JsonObject>(EventBusAddresses.MessageDao.name, JsonObject().apply {
                put(FieldLabels.DaoMethod.name, DAOMethods.UPDATE.name)
                put(MessageParams.KEY.text, it.body().getString(MessageParams.KEY.text))
                put("param", "text2")
            })
        }.flatMap {
            vertx.eventBus().rxSend<JsonObject>(EventBusAddresses.MessageDao.name, JsonObject().apply {
                put(FieldLabels.DaoMethod.name, DAOMethods.GET.name)
                put(MessageParams.KEY.text, it.body().getString(MessageParams.KEY.text))
            })
        }.subscribe({
            val body = it.body()
            assertNotNull(body)
            assertEquals("text2", body.getString("param"))
            context.completeNow()
        }, {
            context.failNow(it)
        })
    }


    @Test
    fun testGetAllMessages(context: VertxTestContext, vertx: Vertx) {
        val message = JsonObject().apply {
            put(FieldLabels.DaoMethod.name, DAOMethods.CREATE.name)
            put("userId", "1")
        }

        vertx.eventBus().rxSend<JsonObject>(EventBusAddresses.MessageDao.name, message).flatMap {
            vertx.eventBus().rxSend<JsonObject>(EventBusAddresses.MessageDao.name, message)
        }.flatMap {
            vertx.eventBus().rxSend<JsonObject>(EventBusAddresses.MessageDao.name, JsonObject().apply {
                put(FieldLabels.DaoMethod.name, DAOMethods.GET_ALL.name)
                put(FieldLabels.Offset.name, 0)
                put(FieldLabels.Limit.name, 10)
                put(MessageParams.USER_ID.text, "1")
            })
        }.subscribe({
            val body = it.body()
            assertNotNull(body)
            assertNotEquals(0, body.getJsonArray(FieldLabels.Data.name).size())
            context.completeNow()
        }, {
            context.failNow(it)
        })
    }
}