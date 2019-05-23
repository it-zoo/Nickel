import DAO.messages.MessageDaoVerticle
import controllers.RestController
import io.vertx.core.DeploymentOptions
import io.vertx.core.VertxOptions
import io.vertx.core.json.JsonObject
import io.vertx.rxjava.core.Vertx
import org.apache.logging.log4j.LogManager
import java.io.BufferedReader
import java.io.FileInputStream
import java.io.FileReader
import java.nio.file.Files
import java.nio.file.Paths

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
     * Load Config from resources
     */
    private fun loadConfig(): JsonObject {
        val file = this::class.java.classLoader.getResource("config.json").file
        val br = BufferedReader(FileReader(file))
        val joinToString = br.readLines().joinToString("")
        br.close()
        return JsonObject(joinToString)
    }

    /**
     * fun used to deploy vertx's verticles
     */
    private fun deployVerticles() {
        val config = loadConfig()
        val options = VertxOptions()
        val deploymentOptions = DeploymentOptions()
        deploymentOptions.config = config
        Vertx.rxClusteredVertx(options).subscribe({
            it.deployVerticle(RestController(), deploymentOptions)
            it.deployVerticle(MessageDaoVerticle(), deploymentOptions)
        }, {
            logger.error("While creating clustered vertx", it)
        })
    }
}