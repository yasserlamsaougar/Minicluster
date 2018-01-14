import com.github.salomonbrys.kodein.Kodein
import com.github.salomonbrys.kodein.bind
import com.github.salomonbrys.kodein.singleton
import component.*
import info.macias.kaconf.Configurator
import info.macias.kaconf.ConfiguratorBuilder
import service.*
import java.util.*


fun main(args: Array<String>) {
    val kodein = Kodein {
        bind<AclService>() with singleton { AclService() }
        bind<ConfigService>() with singleton { ConfigService() }
        bind<FileService>() with singleton { FileService() }
        bind<HdfsFileService>() with singleton { HdfsFileService() }
        bind<JaasService>() with singleton { JaasService() }
    }
    val properties = Properties()
    properties.load(ClassLoader.getSystemClassLoader().getResourceAsStream("cluster.properties"))
    val conf: Configurator = ConfiguratorBuilder()
            .addSource(System.getenv())
            .addSource(System.getProperties())
            .addSource(properties).build()

    val orchestrator = Orchestrator(kodein)
    orchestrator.launch("hbase", "hdfs", "zookeeper", "hive", "yarn", "kafka", configurator = conf)
}
