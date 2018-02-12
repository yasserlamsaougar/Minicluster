package component.core

import com.github.sakserv.minicluster.impl.HbaseLocalCluster
import com.github.salomonbrys.kodein.Kodein
import com.github.salomonbrys.kodein.instance
import component.AbstractComponent
import info.macias.kaconf.Property
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import service.AclService
import service.ConfigService
import service.FileService
import utilities.ClusterID

class HbaseClusterComponent(kodein: Kodein) : AbstractComponent(kodein) {

    @Property("global.basedir")
    val baseDir = "minidata"

    @Property("global.confDir")
    val confDir = "$baseDir/conf"

    @Property("global.secure")
    val secure = false

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

    lateinit var hbaseLocalCluster: HbaseLocalCluster
    override fun launch(configuration: Configuration) {
        val hbaseConfiguration = configure(configuration)
        if (secure) prepareSecure()
        hbaseLocalCluster = HbaseLocalCluster.Builder()
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
        writeConf(hbaseLocalCluster.hbaseConfiguration)
    }

    override fun dependencies(): List<ClusterID> {
        val listOfDependencies = mutableListOf<ClusterID>(
                ClusterID.ZOOKEEPER
        )
        if(secure) listOfDependencies.add(ClusterID.KDC)
        return listOfDependencies
    }

    override fun configuration(): Configuration {
        return hbaseLocalCluster.hbaseConfiguration
    }

    override fun stop() {
        hbaseLocalCluster.stop(true)//To change body of created functions use File | Settings | File Templates.
    }

    override fun clean() {
        val fileService: FileService = kodein.instance()
        fileService.delete("$baseDir/$hbaseRootDir")
    }

    private fun configure(configuration: Configuration): Configuration {
        val hbaseConfiguration = HBaseConfiguration.create()
        hbaseConfiguration.set("hbase.defaults.for.version.skip", "true")
        hbaseConfiguration.addResource(configuration)
        return hbaseConfiguration
    }

    private fun prepareSecure() {
        val aclService: AclService = kodein.instance()
        aclService.giveAllPermissionsToZNode("/hbase-secure", zookeeperConnectionString)
    }

    private fun writeConf(configuration: Configuration) {
        val configService: ConfigService = kodein.instance()
        configService.createConfFile(StringUtils.EMPTY, conf = configuration, outputFilePath = "$confDir/hbase-site.xml")
    }


}