import DAO.messages.MessageDaoVerticle
import controllers.RestController
import io.vertx.core.VertxOptions
import io.vertx.rxjava.core.Vertx
import org.apache.logging.log4j.LogManager

fun main(args: Array<String>) {
    Server()
}


/**
 * Class contains server's starts logic
 * @author kostya05983
 */
class Server {
    private val logger = LogManager.getLogger(RestController::class.java)


    init {
        deployVerticles()
    }

    /**
     * Init logging with this fun
     */
    private fun initLogging() {

    }

    /**
     * fun used to deploy vertx's verticles
     */
    private fun deployVerticles() {
        val options = VertxOptions()
        Vertx.rxClusteredVertx(options).subscribe({
            it.deployVerticle(RestController())
            it.deployVerticle(MessageDaoVerticle())
        }, {
            logger.error("Whiel craeting clkustered verx", it)
        })
    }
}