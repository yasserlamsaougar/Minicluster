package component.core

import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster
import com.github.salomonbrys.kodein.Kodein
import com.github.salomonbrys.kodein.instance
import component.AbstractComponent
import info.macias.kaconf.Property
import org.apache.hadoop.conf.Configuration
import service.FileService
import utilities.ClusterID

class ZookeeperClusterComponent(kodein: Kodein) : AbstractComponent(kodein) {

    @Property("global.basedir")
    val baseDir = "minidata"

    @Property("global.secure")
    val secure = false

    @Property("zookeeper.tempDir")
    val zookeeperTempDir = "embedded_zookeeper"

    @Property("zookeeper.port")
    val zookeeperPort = 6000

    @Property("zookeeper.connectionString")
    val zookeeperConnectionString = "127.0.0.1:6000"

    @Property("zookeeper.maxClientConnections")
    val zookeeperMaxClientConnections = 60

    @Property("zookeeper.electionPort")
    val zookeeperElectionPort = 6001

    @Property("zookeeper.quorumPort")
    val zookeeperQuorumPort = 6002

    @Property("zookeeper.deleteDataDirectoryOnClose")
    val zookeeperDeleteDataDirectoryOnClose = true

    @Property("zookeeper.serverId")
    val zookeeperServerId = 1

    @Property("zookeeperTickTime")
    val zookeeperTickTime = 2000

    lateinit var zookeeperLocalCluster: ZookeeperLocalCluster

    override fun launch(configuration: Configuration) {
        val properties = if (secure) prepareSecure() else HashMap()
        zookeeperLocalCluster = ZookeeperLocalCluster.Builder()
                .setPort(zookeeperPort)
                .setTempDir("$baseDir/$zookeeperTempDir")
                .setZookeeperConnectionString(zookeeperConnectionString)
                .setMaxClientCnxns(zookeeperMaxClientConnections)
                .setElectionPort(zookeeperElectionPort)
                .setQuorumPort(zookeeperQuorumPort)
                .setDeleteDataDirectoryOnClose(zookeeperDeleteDataDirectoryOnClose)
                .setServerId(zookeeperServerId)
                .setTickTime(zookeeperTickTime)
                .setCustomProperties(properties)
                .build()
        zookeeperLocalCluster.start()
    }


    override fun dependencies(): List<ClusterID> {
        val listOfDependencies = mutableListOf<ClusterID>()
        if (secure) listOfDependencies.add(ClusterID.KDC)
        return listOfDependencies
    }

    override fun stop() {
        zookeeperLocalCluster.stop(true)//To change body of created functions use File | Settings | File Templates.
    }

    override fun clean() {
        val fileService:FileService = kodein.instance()
        fileService.delete("$baseDir/$zookeeperTempDir")
    }


    private fun prepareSecure(): HashMap<String, Any> {
        val properties = HashMap<String, Any>()
        properties.put("authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider")
        properties.put("requireClientAuthScheme", "sasl")
        properties.put("sasl.serverconfig", "Server")
        properties.put("kerberos.removeHostFromPrincipal", "true")
        properties.put("kerberos.removeRealmFromPrincipal", "true")

        System.setProperty("zookeeper.sasl.client", "true")
        System.setProperty("zookeeper.sasl.clientconfig", "Client")

        return properties
    }

}