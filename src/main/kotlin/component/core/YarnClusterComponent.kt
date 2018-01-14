package component.core

import com.github.sakserv.minicluster.impl.YarnLocalCluster
import com.github.salomonbrys.kodein.Kodein
import com.github.salomonbrys.kodein.instance
import component.AbstractComponent
import info.macias.kaconf.Property
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import service.ConfigService
import service.FileService
import utilities.ClusterID

class YarnClusterComponent(kodein: Kodein) : AbstractComponent(kodein) {

    @Property("global.basedir")
    val baseDir = "minidata"

    @Property("global.confDir")
    val confDir = "$baseDir/conf"

    @Property("global.secure")
    val secure = false

    @Property("yarn.numNodeManagers")
    val yarnNumNodeManagers = 1

    @Property("yarn.numLocalDirs")
    val yarnNumLocalDirs = 1

    @Property("yarn.numLogDirs")
    val yarnNumLogDirs = 1

    @Property("yarn.resourceManagerAddress")
    val yarnResourceManagerAddress = "127.0.0.1:8032"

    @Property("yarn.resourceManagerHostname")
    val yarnResourceManagerHostname = "127.0.0.1"

    @Property("yarn.resourceManagerSchedulerAddress")
    val yarnResourceManagerSchedulerAddress = "127.0.0.1:47001"

    @Property("yarn.resourceManagerTrackerAddress")
    val yarnResourceManagerTrackerAddress = "127.0.0.1:47002"

    @Property("yarn.resourceManagerWebappAddress")
    val yarnResourceManagerWebappAddress = "127.0.0.1:47003"

    @Property("yarn.useInJvmContainerExecutor")
    val yarnUseInJvmContainerExecutor = false

    lateinit var yarnLocalCluster: YarnLocalCluster

    override fun launch(configuration: Configuration) {
        val copyConfiguration = Configuration(configuration)

        yarnLocalCluster = YarnLocalCluster.Builder()
                .setNumNodeManagers(yarnNumNodeManagers)
                .setNumLocalDirs(yarnNumLocalDirs)
                .setNumLogDirs(yarnNumLogDirs)
                .setResourceManagerAddress(yarnResourceManagerAddress)
                .setResourceManagerHostname(yarnResourceManagerHostname)
                .setResourceManagerSchedulerAddress(yarnResourceManagerSchedulerAddress)
                .setResourceManagerResourceTrackerAddress(yarnResourceManagerTrackerAddress)
                .setResourceManagerWebappAddress(yarnResourceManagerWebappAddress)
                .setUseInJvmContainerExecutor(yarnUseInJvmContainerExecutor)
                .setConfig(copyConfiguration)
                .build()

        yarnLocalCluster.start()
        writeConf(yarnLocalCluster.config)
    }

    override fun dependencies(): List<ClusterID> {
        val listOfDependencies = mutableListOf<ClusterID>(
                ClusterID.HDFS
        )
        if (secure) listOfDependencies.add(ClusterID.KDC)
        return listOfDependencies
    }

    override fun configuration(): Configuration {
        return yarnLocalCluster.config
    }

    override fun stop() {
        yarnLocalCluster.stop(true)//To change body of created functions use File | Settings | File Templates.
    }

    private fun writeConf(configuration: Configuration) {
        val configService: ConfigService = kodein.instance()
        configService.createConfFile(StringUtils.EMPTY, conf = configuration, outputFilePath = "$confDir/yarn-site.xml")
    }


}
