package controllers.messages

import consts.DAOMethods
import consts.EventBusAddresses
import consts.FieldLabels
import consts.messages.MessageParams
import controllers.RestController
import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.core.Handler
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.rxjava.core.Vertx
import io.vertx.rxjava.ext.web.RoutingContext
import org.apache.logging.log4j.LogManager

/**
 * MessageAll handler
 * @author kostya05983
 */
class MessagesAllHandler(private val vertx: Vertx) : Handler<RoutingContext> {
    private val logger = LogManager.getLogger(RestController::class.java)

    override fun handle(event: RoutingContext) {
        when (event.request().method()) {
            HttpMethod.GET -> {
                vertx.eventBus().rxSend<JsonObject>(EventBusAddresses.MessageDao.name, JsonObject().apply {
                    put(FieldLabels.DaoMethod.name, DAOMethods.GET_ALL.name)
                    put(MessageParams.USER_ID.text, event.request().getParam(MessageParams.USER_ID.text))
                    put(FieldLabels.Offset.name, event.request().getParam(FieldLabels.Offset.name).toInt())
                    put(FieldLabels.Limit.name, event.request().getParam(FieldLabels.Limit.name).toInt())
                }).subscribe({
                    event.request().response().end(it.body().encode())
                }, {
                    logger.error("Can't getAll from dao {}", it)
                })
            }
            else -> {
                event.request().response().statusCode = HttpResponseStatus.NOT_FOUND.code()
                event.request().response().end()
                logger.error("No found such method {}")
            }
        }
    }
}