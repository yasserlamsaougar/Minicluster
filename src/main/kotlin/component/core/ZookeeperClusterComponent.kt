package component

import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster
import info.macias.kaconf.Property
import org.apache.hadoop.conf.Configuration

class ZookeeperClusterComponent : AbstractComponent<ZookeeperLocalCluster>() {

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

    override fun launch(configuration: Configuration): ZookeeperLocalCluster {
        val properties = if (secure) prepareSecure() else HashMap()

        val zookeeperLocalCluster = ZookeeperLocalCluster.Builder()
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
        return zookeeperLocalCluster
    }


    fun prepareSecure(): HashMap<String, Any> {
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