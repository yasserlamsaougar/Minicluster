import com.github.salomonbrys.kodein.Kodein
import com.github.salomonbrys.kodein.bind
import com.github.salomonbrys.kodein.singleton
import component.HiveClusterComponent
import component.Orchestrator
import component.core.*
import info.macias.kaconf.Configurator
import info.macias.kaconf.ConfiguratorBuilder
import service.*
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*


fun main(args: Array<String>) {

    val cli = MiniCli(args)
    val configFile = cli.configFile
    val hooksPackages = cli.hooks.split(",").toTypedArray()
    var components = cli.components.split(",").toTypedArray()
    val help = cli.help

    if (help) {
        cli.printHelp(System.out)
        return
    }
    if (cli.components.isEmpty()) {
        components = cli.componentsDefault.split(",").toTypedArray()
    }
    val properties = Properties()

    if (configFile.isEmpty()) {
        properties.load(ClassLoader.getSystemClassLoader().getResourceAsStream("cluster.properties"))
    } else {
        properties.load(Files.newBufferedReader(Paths.get(configFile)))
    }
    val conf: Configurator = ConfiguratorBuilder()
            .addSource(System.getenv())
            .addSource(System.getProperties())
            .addSource(properties).build()

    val kodein = Kodein {
        bind() from singleton { AclService() }
        bind() from singleton { ConfigService() }
        bind() from singleton { FileService() }
        bind() from singleton { HdfsFileService() }
        bind() from singleton { JaasService() }
        bind() from singleton { HbaseClusterComponent(kodein) }
        bind() from singleton { HdfsClusterComponent(kodein) }
        bind() from singleton { HiveClusterComponent(kodein) }
        bind() from singleton { HiveMetastoreClusterComponent(kodein) }
        bind() from singleton { KafkaClusterComponent(kodein) }
        bind() from singleton { KdcClusterComponent(kodein) }
        bind() from singleton { YarnClusterComponent(kodein) }
        bind() from singleton { ZookeeperClusterComponent(kodein) }
    }
    val orchestrator = Orchestrator(kodein)
    orchestrator.launch(*components, hooksPackages = hooksPackages, configurator = conf)
}
