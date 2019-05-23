package controllers

import controllers.messages.MessageHandler
import controllers.messages.MessagesAllHandler
import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.ext.web.Router
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
        messageHandler = MessageHandler(vertx)
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
        router.route(MESSAGES).handler(messageHandler)
        router.route(MESSAGES_ALL).handler(messageAllHandler)
        return router
    }
}