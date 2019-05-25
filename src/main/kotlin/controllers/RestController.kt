package controllers

import consts.ConfigParameters
import controllers.messages.MessageHandler
import controllers.messages.MessagesAllHandler
import io.vertx.core.http.HttpMethod
import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.ext.web.Router
import io.vertx.rxjava.ext.web.handler.BodyHandler
import io.vertx.rxjava.ext.web.handler.CorsHandler
import org.apache.logging.log4j.LogManager
import rx.Completable

/**
 * Controller for routes
 */
class RestController : AbstractVerticle() {
    private val logger = LogManager.getLogger(RestController::class.java)

    companion object {
        private const val MESSAGES: String = "/messages"
        private const val MESSAGES_ALL: String = "/messages/all"
    }

    private lateinit var messageHandler: MessageHandler
    private lateinit var messageAllHandler: MessagesAllHandler


    override fun rxStart(): Completable {
        val host = config().getString(ConfigParameters.MardukHost.name)
        val port = config().getInteger(ConfigParameters.MardukPort.name)

        messageHandler = MessageHandler(vertx, "http://$host:$port")
        messageAllHandler = MessagesAllHandler(vertx)
        val router = initRouter()

        return vertx.createHttpServer().requestHandler {
            router.accept(it)
        }.rxListen(8080).toCompletable()
    }

    /**
     * Initialize all routes in this fun
     */
    private fun initRouter(): Router {
        val router = Router.router(vertx)

        val allowedHeaders = HashSet<String>()
        allowedHeaders.add("x-request-with")
        allowedHeaders.add("Access-Control-Allow-Origin")
        allowedHeaders.add("origin")
        allowedHeaders.add("Content-type")
        allowedHeaders.add("accept")
        allowedHeaders.add("X-PINGARUNER")

        val allowerMethods = HashSet<HttpMethod>()
        allowerMethods.add(HttpMethod.GET)
        allowerMethods.add(HttpMethod.POST)
        allowerMethods.add(HttpMethod.PUT)
        allowerMethods.add(HttpMethod.DELETE)

        val corsHandler = CorsHandler.create("*")
        corsHandler.allowedHeaders(allowedHeaders)
        corsHandler.allowedMethod(HttpMethod.GET)
        corsHandler.allowedMethod(HttpMethod.POST)
        corsHandler.allowedMethod(HttpMethod.PUT)
        corsHandler.allowedMethod(HttpMethod.DELETE)
        router.route().handler {
            corsHandler.handle(it)
        }

        router.route().handler(BodyHandler.create())

        router.route(MESSAGES).handler(messageHandler)
        router.route(MESSAGES_ALL).handler(messageAllHandler)
        return router
    }
}