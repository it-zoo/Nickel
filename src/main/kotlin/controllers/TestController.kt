package controllers

import io.vertx.core.Future
import io.vertx.core.http.HttpHeaders
import io.vertx.core.http.HttpServerRequest
import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.ext.web.Router
import io.vertx.rxjava.ext.web.RoutingContext
import org.apache.logging.log4j.LogManager
import rx.Completable

/**
 * Controller for funny testing
 */
class TestController : AbstractVerticle() {
    private val logger = LogManager.getLogger(TestController::class.java)

    companion object {
        private const val test: String = "/test"
    }

//    /**
//     * This func call by vertx, when verticle is deployed
//     */
//    override fun rxStart(): Completable {
//        return super.rxStart()
//    }

    override fun start(startFuture: Future<Void>) {
        val router = initRouter()
        vertx.createHttpServer().requestHandler{
            router.accept(it)
        }.listen(8080) {
            if(it.succeeded()) {
                logger.debug("I'm fine body")
                startFuture.complete()
            } else {
                logger.error("Fuck I'm bad")
                startFuture.fail(it.cause())
            }
        }
        initRouter()
    }

    /**
     * Initialize all routes in this fun
     */
    private fun initRouter(): Router {
        val router = Router.router(vertx)
        router.get(test).handler(this::testRoute)
        return router
    }

    /**
     * Just for fun
     */
    private fun testRoute(context: RoutingContext) {
        val key = context.get<String>("Key")
        logger.debug("Xuyak get  key $key")
        val response = context.response()
        response.putHeader(HttpHeaders.CONTENT_TYPE, "application/json").end(""" {"status": "ok"} """)
    }
}