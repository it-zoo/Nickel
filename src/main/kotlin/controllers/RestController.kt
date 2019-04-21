package controllers

import io.vertx.core.Future
import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.ext.web.Router
import org.apache.logging.log4j.LogManager
import rx.Completable

/**
 * Controller for funny testing
 */
class RestController : AbstractVerticle() {
    private val logger = LogManager.getLogger(RestController::class.java)

    companion object {
        private const val test: String = "/test"
    }


    override fun rxStart(): Completable {
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
        router.route(test).handler(MessageHandler())
        return router
    }
}