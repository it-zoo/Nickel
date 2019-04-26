import DAO.MessageDaoVerticle
import com.arangodb.ArangoCollectionAsync
import com.arangodb.ArangoDBAsync
import com.arangodb.ArangoDatabaseAsync
import consts.*
import io.vertx.core.DeploymentOptions
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.rxjava.core.Vertx
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.extension.ExtendWith

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension::class)
class MessageDaoVerticleTest {

    private lateinit var arangoDb: ArangoDBAsync
    private lateinit var arangoDatabaseAsync: ArangoDatabaseAsync
    private lateinit var arangoCollectionAsync: ArangoCollectionAsync


    @BeforeAll
    fun setUp(vertx: Vertx, context: VertxTestContext) {
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
        json.put(ConfigParameters.ArangoDbHost.name, "loclahost")
        json.put(ConfigParameters.ArangoDbPort.name, "6767")
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
                    val s = it.body()
                    assertEquals("Success", s)
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
    fun testGetMessage(context: VertxTestContext) {

    }

    @Test
    fun testGetAllMessages(context: VertxTestContext) {

    }
}