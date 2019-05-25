package controllers.messages

import consts.DAOMethods
import consts.EventBusAddresses
import consts.FieldLabels
import consts.messages.MessageParams
import controllers.RestController
import io.vertx.core.Handler
import io.vertx.core.http.HttpHeaders
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.rxjava.core.Vertx
import io.vertx.rxjava.ext.web.RoutingContext
import org.apache.logging.log4j.LogManager

class MessageHandler(private val vertx: Vertx) : Handler<RoutingContext> {
    private val logger = LogManager.getLogger(RestController::class.java)

    override fun handle(event: RoutingContext) {
        when (event.request().method()) {
            HttpMethod.GET -> getMessage(event)
            HttpMethod.POST -> createMessage(event)
            HttpMethod.PUT -> updateMessage(event)
            HttpMethod.DELETE -> deleteMessage(event)
            else -> {
                logger.error("No such method in message route")
                event.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json").end(" No such method  or not implemented")
            }
        }
    }

    private fun getMessage(event: RoutingContext) {
        vertx.eventBus().rxSend<JsonObject>(EventBusAddresses.MessageDao.name, JsonObject().apply {
            put(FieldLabels.DaoMethod.name, DAOMethods.GET.name)
            put(MessageParams.KEY.name, event.request().getParam(MessageParams.KEY.text))
        }).subscribe({
            event.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json").end(it.body().encode())
        }, {
            logger.error("Can't get such message", it)
        })
    }

    private fun createMessage(event: RoutingContext) {
        val message = event.bodyAsJson
        message.put(FieldLabels.DaoMethod.name, DAOMethods.CREATE.name)
        vertx.eventBus().rxSend<JsonObject>(EventBusAddresses.MessageDao.name, message).subscribe({
            event.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json").end(it.body().encode())
        }, {
            logger.error("Error while create such message", it)
        })
    }

    private fun updateMessage(event: RoutingContext) {
        val message = event.bodyAsJson
        message.put(FieldLabels.DaoMethod.name, DAOMethods.UPDATE.name)
        vertx.eventBus().rxSend<JsonObject>(EventBusAddresses.MessageDao.name, message).subscribe({
            event.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json").end(it.body().encode())
        }, {
            logger.error("Error while create such message", it)
        })
    }

    private fun deleteMessage(event: RoutingContext) {
        vertx.eventBus().rxSend<JsonObject>(EventBusAddresses.MessageDao.name, JsonObject().apply {
            put(FieldLabels.DaoMethod.name, DAOMethods.DELETE.name)
            put(MessageParams.KEY.text, event.request().getParam(MessageParams.KEY.text))
        }).subscribe({
            event.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json").end(it.body().encode())
        }, {
            logger.error("Can't send and add to dao {}", it)
        })
    }
}