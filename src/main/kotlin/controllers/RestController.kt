package controllers

import controllers.messages.MessageHandler
import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.ext.web.Router
import org.apache.logging.log4j.LogManager
import rx.Completable
import java.util.concurrent.ConcurrentHashMap

/**
 * Controller for funny testing
 */
class RestController : AbstractVerticle() {
    private val logger = LogManager.getLogger(RestController::class.java)

    companion object {
        private const val MESSAGES: String = "/messages"
    }

    private lateinit var messageHandler: MessageHandler


    override fun rxStart(): Completable {
        messageHandler = MessageHandler(vertx)
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
        return router
    }
}