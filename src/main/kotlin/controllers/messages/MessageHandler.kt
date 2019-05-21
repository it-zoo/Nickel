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
            HttpMethod.GET -> {
                vertx.eventBus().rxSend<JsonObject>(EventBusAddresses.MessageDao.name, JsonObject().apply {
                    put(FieldLabels.DaoMethod.name, DAOMethods.GET.name)
                    put(MessageParams.KEY.name, event.request().getParam(MessageParams.KEY.text))
                }).subscribe({
                    event.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json").end(it.body().toString())
                }, {
                    logger.error("Can't get such message", it)
                })
            }
            HttpMethod.POST -> {
                event.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json").end(""" Create """)
            }
            HttpMethod.PUT -> {

                event.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json").end(""" Update """)
            }
            HttpMethod.DELETE -> {
                vertx.eventBus().rxSend<JsonObject>(EventBusAddresses.MessageDao.name, JsonObject().apply {
                    put(FieldLabels.DaoMethod.name, DAOMethods.DELETE.name)
                    put(MessageParams.KEY.text, event.request().getParam(MessageParams.KEY.text))
                }).subscribe({
                    event.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json").end("""Delete with key""")
                }, {
                    logger.error("Can't send and add to dao {}", it)
                })
            }
            else -> {
                logger.error("No such method in message route")
                event.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json").end(""" No such method """)
            }
        }
    }
}