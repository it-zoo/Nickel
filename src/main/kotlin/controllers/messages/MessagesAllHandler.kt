package controllers.messages

import consts.DAOMethods
import consts.EventBusAddresses
import consts.FieldLabels
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
                val message = event.bodyAsJson
                vertx.eventBus().rxSend<JsonObject>(EventBusAddresses.MessageDao.name, message.put(FieldLabels.DaoMethod.name, DAOMethods.GET_ALL.name)).subscribe({
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