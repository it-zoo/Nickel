package DAO.messages

import DAO.ToJsonArray
import com.arangodb.ArangoCollectionAsync
import com.arangodb.ArangoDBAsync
import com.arangodb.ArangoDatabaseAsync
import com.arangodb.entity.BaseDocument
import com.arangodb.util.MapBuilder
import consts.*
import consts.messages.MessageParams
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
        private val FIND_ALL_MESSAGES_QUERY = """
        FOR document in ${ArangoCollections.Messages.name}
                FILTER document.userId==@userId
                LIMIT @offset, @limit
                return document
        """
    }

    override fun rxStart(): Completable {
        initEventBus()
        val config = config()

        val host = config.getString(ConfigParameters.ArangoDbHost.name)
        val port = config.getInteger(ConfigParameters.ArangoDbPort.name)
        return Completable.fromCallable {
            initDatabase(host, port)
        }
    }

    private fun initDatabase(host: String, port: Int) {
        arangoDb = ArangoDBAsync
                .Builder()
                .host(host, port)
                .build()
        if (!arangoDb.databases.get().contains(ArangoDatabases.Client.name)) {
            arangoDb.createDatabase(ArangoDatabases.Client.name).get()
        }
        arangoDatabaseAsync = arangoDb.db(ArangoDatabases.Client.name)
        val collections = arangoDatabaseAsync.collections.get().map { it.name }
        if (!collections.contains(ArangoCollections.Messages.name)) {
            arangoDatabaseAsync.createCollection(ArangoCollections.Messages.name).get()
        }
        arangoCollectionAsync = arangoDatabaseAsync.collection(ArangoCollections.Messages.name)
    }

    private fun initEventBus() {
        vertx.eventBus().consumer<JsonObject>(EventBusAddresses.MessageDao.name, this::switchRoute)
    }

    private fun switchRoute(message: Message<JsonObject>) {
        when (message.body().getString(FieldLabels.DaoMethod.name)) {
            DAOMethods.GET_ALL.name -> getAllMessages(message)

            DAOMethods.GET.name -> getMessage(message)

            DAOMethods.CREATE.name -> createMessage(message)

            DAOMethods.DELETE.name -> deleteMessage(message)

            DAOMethods.UPDATE.name -> updateMessage(message)

            else -> {
                message.fail(ErrorCode.NO_SUCH_MESSAGE.code, "Not implemented")
            }
        }
    }

    private fun createMessage(message: Message<JsonObject>) {
        val body = message.body()
        val document = BaseDocument()
        document.properties = body.map

        arangoCollectionAsync.insertDocument(document).whenComplete { doc, ex ->
            if (doc.key != null) {
                arangoCollectionAsync.getDocument(doc.key, BaseDocument::class.java).whenComplete { t, u ->
                    message.reply(JsonObject().apply {
                        put(MessageParams.KEY.text, doc.key)
                        t.properties.forEach {
                            put(it.key, it.value)
                        }
                    })
                }
            } else {
                message.fail(ErrorCode.CREATE_MESSAGE.code, "Fail to get document")
            }
        }
    }

    /**
     * Get message from collection
     */
    private fun getMessage(message: Message<JsonObject>) {
        val key = message.body().getString(MessageParams.KEY.text)

        arangoCollectionAsync.getDocument(key, BaseDocument::class.java).whenComplete { t, u ->
            if (u == null) {
                if (t != null) {
                    val json = JsonObject(t.properties)
                    json.put(MessageParams.KEY.text, t.key)
                    message.reply(json)
                } else {
                    message.reply(t)
                }
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
                .put("offset", json.getInteger(FieldLabels.Offset.name))
                .put("limit", json.getInteger(FieldLabels.Limit.name))
                .put("userId", json.getString(MessageParams.USER_ID.text).toInt()).get()

        arangoDatabaseAsync
                .query(FIND_ALL_MESSAGES_QUERY, params, BaseDocument::class.java)
                .whenComplete { t, u ->
                    if (t != null) {
                        val jsonArray = t.streamRemaining()
                                .collect(ToJsonArray())

                        message.reply(JsonObject().apply {
                            put(FieldLabels.Data.name, jsonArray)
                        })
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
        val key = json.getString(MessageParams.KEY.text)

        arangoCollectionAsync.deleteDocument(key).whenComplete { t, u ->
            if (t != null) {
                message.reply(JsonObject().apply {
                    put(MessageParams.KEY.text, t.key)
                })
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

        arangoCollectionAsync.updateDocument(json.getString(MessageParams.KEY.text), document).whenComplete { t, u ->
            if (t != null) {
                val response = JsonObject()
                response.put(MessageParams.KEY.text, t.key)
                message.reply(response)
            } else {
                message.fail(ErrorCode.UPDATE_MESSAGE.code, "Fail to update message")
            }
        }
    }

    private fun mappingToDocument(json: JsonObject): BaseDocument {
        val document = BaseDocument()
        json.forEach {
            document.addAttribute(it.key, it.value)
        }
        return document
    }
}