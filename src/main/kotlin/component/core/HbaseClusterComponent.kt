package component

import com.github.sakserv.minicluster.impl.HbaseLocalCluster
import com.github.salomonbrys.kodein.KodeinInjector
import com.github.salomonbrys.kodein.instance
import info.macias.kaconf.Property
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import service.AclService

class HbaseClusterComponent : Component<HbaseLocalCluster> {

    @Property("global.basedir")
    val baseDir = "minidata"

    @Property("global.secure")
    val secure = false

    @Property("kerberos.tempDir")
    val kerberosTempDir = "embedded_kdc"

    @Property("hbase.masterPort")
    val hbaseMasterPort = 25000

    @Property("hbase.masterInfoPort")
    val hbaseMasterInfoPort = 25010

    @Property("hbase.numRegionServers")
    val hbaseNumRegionServers = 1

    @Property("hbase.rootDir")
    val hbaseRootDir = "embedded_hbase"

    @Property("zookeeper.port")
    val zookeeperPort = 6000

    @Property("zookeeper.connectionString")
    val zookeeperConnectionString = "127.0.0.1:6000"

    @Property("hbase.zookeeperZnodeParent")
    val hbaseZookeeperZnodeParent = "/hbase-secure"

    @Property("hbase.walReplicationEnabled")
    val hbaseWalReplicationEnabled = false

    @Property("hbase.restHost")
    val hbaseRestHost = "127.0.0.1"

    @Property("hbase.restPort")
    val hbaseRestPort = 25020

    @Property("hbase.restReadOnly")
    val hbaseRestReadOnly = false

    @Property("hbase.restThreadMax")
    val hbaseRestThreadMax = 100

    @Property("hbase.restThreadMin")
    val hbaseRestThreadMin = 2

    override val injector: KodeinInjector = KodeinInjector()

    override fun launch(configuration: Configuration) : HbaseLocalCluster{
        val hbaseConfiguration = configure(configuration)
        if (secure) prepareSecure()
        val hbaseLocalCluster = HbaseLocalCluster.Builder()
                .setHbaseMasterPort(hbaseMasterPort)
                .setHbaseMasterInfoPort(hbaseMasterInfoPort)
                .setNumRegionServers(hbaseNumRegionServers)
                .setHbaseRootDir("$baseDir/$hbaseRootDir")
                .setZookeeperPort(zookeeperPort)
                .setZookeeperConnectionString(zookeeperConnectionString)
                .setZookeeperZnodeParent(hbaseZookeeperZnodeParent)
                .setHbaseWalReplicationEnabled(hbaseWalReplicationEnabled)
                .setHbaseConfiguration(hbaseConfiguration)
                .activeRestGateway()
                .setHbaseRestHost(hbaseRestHost)
                .setHbaseRestPort(hbaseRestPort)
                .setHbaseRestReadOnly(hbaseRestReadOnly)
                .setHbaseRestThreadMax(hbaseRestThreadMax)
                .setHbaseRestThreadMin(hbaseRestThreadMin)
                .build()
                .build()
        hbaseLocalCluster.start()
        return hbaseLocalCluster
    }

    private fun configure(configuration: Configuration) : Configuration{
        val hbaseConfiguration = HBaseConfiguration.create()
        hbaseConfiguration.set("hbase.defaults.for.version.skip", "true")
        hbaseConfiguration.addResource(configuration)
        return hbaseConfiguration
    }

    fun prepareSecure() {
        val aclService:AclService by instance()
        aclService.giveAllPermissionsToZNode("/hbase-secure", zookeeperConnectionString)
    }


}