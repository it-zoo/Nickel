package DAO

import com.arangodb.ArangoCollectionAsync
import com.arangodb.ArangoDBAsync
import com.arangodb.ArangoDatabaseAsync
import com.arangodb.entity.BaseDocument
import com.arangodb.util.MapBuilder
import consts.*
import io.vertx.core.json.JsonObject
import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.core.eventbus.Message
import rx.Completable
import java.time.ZonedDateTime
import java.util.*

/**
 * @author kostya05983
 */
class MessageDaoVerticle : AbstractVerticle() {

    private lateinit var arangoDb: ArangoDBAsync
    private lateinit var arangoDatabaseAsync: ArangoDatabaseAsync
    private lateinit var arangoCollectionAsync: ArangoCollectionAsync


    companion object {
        private const val FIND_ALL_MESSAGES_QUERY = """
        FOR message in @@collection
        LIMIT @@offset, @@limit
        RETURN message
        """

        private const val REMOVE_MESSAGE = """
           REMOVE @@key in @@collection
        """

        private const val UPDATE_MESSAGE = """
           UPDATE @@document in @@collection
        """
    }

    override fun rxStart(): Completable {
        initEventBus()
        val config = config()

        val host = config.getString(ConfigParameters.ArangoDbHost.name)
        val port = config.getInteger(ConfigParameters.ArangoDbPort.name)
        initDatabase(host, port)

        return super.rxStart()
    }

    private fun initDatabase(host: String, port: Int) {
        arangoDb = ArangoDBAsync
                .Builder()
                .host(host, port)
                .build()
        arangoDb.createDatabase(ArangoDatabases.Client.name).get()
        arangoDatabaseAsync = arangoDb.db(ArangoDatabases.Client.name)
        arangoDatabaseAsync.createCollection(ArangoCollections.Messages.name).get()
        arangoCollectionAsync = arangoDatabaseAsync.collection(ArangoCollections.Messages.name)
    }

    private fun initEventBus() {
        vertx.eventBus().consumer<JsonObject>("Test")
        vertx.eventBus().consumer<JsonObject>(EventBusAddresses.MessageDao.name, this::switchRoute)
    }

    private fun switchRoute(message: Message<JsonObject>) {
        val method = message.body().getString(FieldLabels.DaoMethod.name)
        when (method) {
            DAOMethods.GET_ALL.name -> getAllMessages(message)

            DAOMethods.GET.name -> getMessage(message)

            DAOMethods.CREATE.name -> createMessage(message)

            DAOMethods.DELETE.name -> deleteMessage(message)

            DAOMethods.UPDATE.name -> updateMessage(message)
        }
    }

    private fun createMessage(message: Message<JsonObject>) {
        val body = message.body()
        val model = models.Message(UUID.randomUUID().toString(), body.getString(FieldLabels.Data.name),
                ZonedDateTime.parse(body.getString(FieldLabels.Time.name)))

        arangoCollectionAsync.insertDocument(model).whenComplete { doc, ex ->
            if (doc.key != null) {
                message.reply("Success")
            } else {
                message.fail(ErrorCode.CREATE_MESSAGE.code, "Fail to get document")
            }
        }
    }

    /**
     * Get message from collection
     */
    private fun getMessage(message: Message<JsonObject>) {
        val key = message.body().getString(FieldLabels.Key.name)

        arangoCollectionAsync.getDocument(key, models.Message::class.java).whenComplete { t, u ->
            if (u != null) {
                message.reply(t)
            } else {
                message.fail(ErrorCode.GET_MESSAGE.code, "Fail to get document!!!!")
            }
        }
    }

    /**
     * Get all messages from db
     */
    private fun getAllMessages(message: Message<JsonObject>) {
        val json = message.body()

        val params = MapBuilder()
                .put("@collection", ArangoCollections.Messages.name)
                .put("@offset", json.getInteger(FieldLabels.Offset.name))
                .put("@limit", json.getInteger(FieldLabels.Limit.name)).get()

        arangoDatabaseAsync
                .query(FIND_ALL_MESSAGES_QUERY, params, BaseDocument::class.java)
                .whenComplete { t, u ->
                    if (u != null) {
                        val jsonArray = t.streamRemaining()
                                .collect(ToJsonArray())
                        message.reply(jsonArray)
                    } else {
                        message.fail(ErrorCode.GET_ALL_MESSAGE.code, "Fail to get all messages")
                    }
                }
    }

    /**
     * Delete message by id
     */
    private fun deleteMessage(message: Message<JsonObject>) {
        val json = message.body()
        val key = json.getString(FieldLabels.Key.name)

        val params = MapBuilder()
                .put("@collection", ArangoCollections.Messages.name)
                .put("@key", key)
                .get()

        arangoDatabaseAsync.query(REMOVE_MESSAGE, params, BaseDocument::class.java)
                .whenComplete { t, u ->
                    if (u != null) {
                        message.reply("Successfully delete message")
                    } else {
                        message.fail(ErrorCode.DELETE_MESSAGE.code, "Fail to delete message by id")
                    }
                }
    }

    /**
     * Update document in collections of messages
     */
    private fun updateMessage(message: Message<JsonObject>) {
        val json = message.body()

        val document = mappingToDocument(json)

        val params = MapBuilder()
                .put("@document", document)
                .put("@collection", ArangoCollections.Messages.name)
                .get()

        arangoDatabaseAsync.query(UPDATE_MESSAGE, params, BaseDocument::class.java)
                .whenComplete { t, u ->
                    if (u != null) {
                        message.reply("Successfully update message in db")
                    } else {
                        message.fail(ErrorCode.UPDATE_MESSAGE.code, "Fail to update message")
                    }
                }
    }

    private fun mappingToDocument(json: JsonObject): BaseDocument {
        val document = BaseDocument()
//        document.addAttribute
//        document.addAttribute(FieldLabels)


        return document
    }
}