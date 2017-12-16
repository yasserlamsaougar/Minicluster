package component

import com.github.sakserv.minicluster.impl.YarnLocalCluster
import com.github.salomonbrys.kodein.Kodein
import info.macias.kaconf.Property
import org.apache.hadoop.conf.Configuration

class YarnClusterComponent : AbstractComponent<YarnLocalCluster>() {

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


    override fun launch(configuration: Configuration): YarnLocalCluster {
        val copyConfiguration = Configuration(configuration)
        val yarnLocalCluster = YarnLocalCluster.Builder()
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
        return yarnLocalCluster
    }

}
