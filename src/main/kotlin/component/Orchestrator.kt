package component

import com.github.salomonbrys.kodein.Kodein
import com.github.salomonbrys.kodein.instance
import component.core.*
import hooks.Hook
import info.macias.kaconf.Configurator
import org.apache.hadoop.conf.Configuration
import org.reflections.Reflections
import utilities.ClusterID

class Orchestrator(val kodein: Kodein) {

    data class ClusterData(val cluster: Component, val clusterId: ClusterID, var resolved: Boolean = false)

    private val idComponentMap = hashMapOf(
            ClusterID.ZOOKEEPER to ClusterData(kodein.instance<ZookeeperClusterComponent>(), ClusterID.ZOOKEEPER),
            ClusterID.HIVEMETASTORE to ClusterData(kodein.instance<HiveMetastoreClusterComponent>(), ClusterID.HIVEMETASTORE),
            ClusterID.HIVE to ClusterData(kodein.instance<HiveClusterComponent>(), ClusterID.HIVE),
            ClusterID.HDFS to ClusterData(kodein.instance<HdfsClusterComponent>(), ClusterID.HDFS),
            ClusterID.KAFKA to ClusterData(kodein.instance<KafkaClusterComponent>(), ClusterID.KAFKA),
            ClusterID.KDC to ClusterData(kodein.instance<KdcClusterComponent>(), ClusterID.KDC),
            ClusterID.HBASE to ClusterData(kodein.instance<HbaseClusterComponent>(), ClusterID.HBASE),
            ClusterID.YARN to ClusterData(kodein.instance<YarnClusterComponent>(), ClusterID.YARN)
    )

    fun launch(vararg ids: String, hooksPackages: Array<String>, configurator: Configurator) {
        idComponentMap.forEach { (_, v) ->
            configurator.configure(v.cluster)
        }
        val filteredHooks = hooksPackages.filter { it.trim().isNotEmpty() }.toTypedArray()
        val clusterIds = clusterIdToClusterData(ids.map(String::toUpperCase).map(ClusterID::valueOf))
        val hooks = getHooks(packageNames = *filteredHooks)
        launchBefore(hooks)
        launchRecursive(clusterIds)
        launchAfter(hooks)
    }

    fun stop(vararg ids: String) {
        val clusterIds: List<ClusterData> = clusterIdToClusterData(ids.map(String::toUpperCase).map(ClusterID::valueOf))
        clusterIds.forEach { clusterId ->
            clusterId.cluster.stop()
            clusterId.resolved = false
        }
    }

    private fun launchRecursive(clustersData: List<ClusterData>) {
        clustersData.forEach { clusterData ->
            val dependencyResolved = clusterData.resolved
            if (!dependencyResolved) {
                val clusterDependencies = clusterIdToClusterData(clusterData.cluster.dependencies())
                val clusterDependenciesFiltered = clusterDependencies.filter { e -> !e.resolved }
                if (!clusterDependenciesFiltered.isEmpty()) {
                    launchRecursive(clusterDependenciesFiltered)
                }
                val configuration = Configuration()
                clusterDependencies.forEach { (cluster) ->
                    configuration.addResource(cluster.configuration())
                }
                clusterData.cluster.clean()
                clusterData.cluster.launch(configuration)
                clusterData.resolved = true
            }
        }
    }

    private fun getHooks(vararg packageNames: String): List<Hook> {
        val setOfPackages = setOf(*packageNames, "hooks")
        val reflections = Reflections(setOfPackages)
        return reflections.getSubTypesOf(Hook::class.java).map {
            it.newInstance()
        }
    }

    private fun launchBefore(hooks: List<Hook>) {
        hooks.forEach { it.before(kodein) }
    }

    private fun launchAfter(hooks: List<Hook>) {
        hooks.forEach { it.after(kodein) }
    }

    private fun clusterIdToClusterData(ids: List<ClusterID>): List<ClusterData> {
        return ids.filter { idComponentMap.containsKey(it) }.map { e ->
            val clusterData = idComponentMap[e]
            clusterData
        }.requireNoNulls()
    }
}

