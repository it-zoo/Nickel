package DAO.messages

import com.arangodb.ArangoCollectionAsync
import com.arangodb.ArangoDBAsync
import com.arangodb.ArangoDatabaseAsync
import consts.*
import consts.messages.MessageParams
import io.vertx.core.DeploymentOptions
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.rxjava.core.Vertx
import org.junit.Rule
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
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

    private lateinit var arangoDb: ArangoDBAsync
    private lateinit var arangoDatabaseAsync: ArangoDatabaseAsync
    private lateinit var arangoCollectionAsync: ArangoCollectionAsync
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

        arangoDb = ArangoDBAsync.Builder().build()
        arangoDb.createDatabase(ArangoDatabases.Client.name).get()
        arangoDatabaseAsync = arangoDb.db(ArangoDatabases.Client.name)
        arangoDatabaseAsync.createCollection(ArangoCollections.Messages.name).get()
        arangoCollectionAsync = arangoDatabaseAsync.collection(ArangoCollections.Messages.name)

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

    private fun addDocumentToDb(model: models.Message) = arangoCollectionAsync.insertDocument(model)

    @Test
    fun testCreateMessage(vertx: Vertx, context: VertxTestContext) {
        val messageToVerticle = JsonObject().apply {
            put(FieldLabels.DaoMethod.name, DAOMethods.CREATE.name)
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
            put(FieldLabels.DaoMethod.name, DAOMethods.CREATE)
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
            assertEquals(body.getString("test"), "param")
            context.completeNow()
        }, {
            context.failNow(it)
        })
    }

    @Test
    fun testDeleteMessage(context: VertxTestContext) {

    }

    @Test
    fun testUpdateMessage(context: VertxTestContext) {

    }


    @Test
    fun testGetAllMessages(context: VertxTestContext) {

    }
}