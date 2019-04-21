package DAO

import com.arangodb.ArangoCollectionAsync
import com.arangodb.ArangoDBAsync
import com.arangodb.ArangoDatabaseAsync
import consts.*
import io.vertx.core.json.JsonObject
import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.core.eventbus.Message
import rx.Completable
import java.time.ZonedDateTime

/**
 * @author kostya05983
 */
class MessageDaoVerticle : AbstractVerticle() {

    lateinit var arangoDb: ArangoDBAsync
    lateinit var arangoDatabaseAsync: ArangoDatabaseAsync
    lateinit var arangoCollectionAsync: ArangoCollectionAsync


    override fun rxStart(): Completable {
        initEventBus()
        initDatabase()
        return super.rxStart()
    }

    private fun initDatabase() {
        arangoDb = ArangoDBAsync.Builder().build()
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
        val model = models.Message(body.getString(FieldLabels.Data.name),
                ZonedDateTime.parse(body.getString(FieldLabels.Time.name)))

        arangoCollectionAsync.insertDocument(model).whenComplete { doc, ex ->
            if (doc.key != null) {
                message.reply("Success")
            } else {
                message.reply("Fail")
            }
        }
    }

    private fun getMessage(message: Message<JsonObject>) {

    }

    private fun getAllMessages(message: Message<JsonObject>) {

    }

    private fun deleteMessage(message: Message<JsonObject>) {

    }

    private fun updateMessage(message: Message<JsonObject>) {

    }
}