package component

import com.github.salomonbrys.kodein.Kodein
import component.core.*
import info.macias.kaconf.Configurator
import org.apache.hadoop.conf.Configuration
import utilities.ClusterID

class Orchestrator(val kodein: Kodein) {

    data class ClusterData(val cluster: Component, val clusterId: ClusterID, var resolved: Boolean = false)

    private val idComponentMap = hashMapOf(
            ClusterID.ZOOKEEPER to ClusterData(ZookeeperClusterComponent(kodein), ClusterID.ZOOKEEPER),
            ClusterID.HIVEMETASTORE to ClusterData(HiveMetastoreClusterComponent(kodein), ClusterID.HIVEMETASTORE),
            ClusterID.HIVE to ClusterData(HiveClusterComponent(kodein), ClusterID.HIVE),
            ClusterID.HDFS to ClusterData(HdfsClusterComponent(kodein), ClusterID.HDFS),
            ClusterID.KAFKA to ClusterData(KafkaClusterComponent(kodein), ClusterID.KAFKA),
            ClusterID.KDC to ClusterData(KdcClusterComponent(kodein), ClusterID.KDC),
            ClusterID.HBASE to ClusterData(HbaseClusterComponent(kodein), ClusterID.HBASE),
            ClusterID.YARN to ClusterData(YarnClusterComponent(kodein), ClusterID.YARN)
    )

    fun launch(vararg ids: String, configurator: Configurator) {
        idComponentMap.forEach { (_, v) ->
            configurator.configure(v.cluster)
        }
        val clusterIds = clusterIdToClusterData(ids.map(String::toUpperCase).map(ClusterID::valueOf))

        launchRecursive(clusterIds)
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

    private fun clusterIdToClusterData(ids: List<ClusterID>): List<ClusterData> {
        return ids.map { e ->
            val clusterData = idComponentMap[e]
            clusterData
        }.requireNoNulls()
    }
}

