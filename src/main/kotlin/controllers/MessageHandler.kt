package controllers

import io.vertx.core.Handler
import io.vertx.core.http.HttpHeaders
import io.vertx.core.http.HttpMethod
import io.vertx.rxjava.ext.web.RoutingContext
import org.apache.logging.log4j.LogManager

class MessageHandler : Handler<RoutingContext> {
    private val logger = LogManager.getLogger(RestController::class.java)

    override fun handle(event: RoutingContext) {
        val method = event.request().method()
        when (method) {
            HttpMethod.GET -> {
                event.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json").end(""" Ok body """)
            }
            HttpMethod.POST -> {

            }
            HttpMethod.PUT -> {

            }
            HttpMethod.DELETE -> {

            }
            else -> {
                logger.error("No such method in message route")
                event.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json").end(""" No such method """)
            }
        }
    }




}