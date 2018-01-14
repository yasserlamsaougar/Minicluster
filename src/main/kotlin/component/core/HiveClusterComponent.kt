package component

import com.github.sakserv.minicluster.impl.HiveLocalServer2
import com.github.salomonbrys.kodein.Kodein
import com.github.salomonbrys.kodein.instance
import info.macias.kaconf.Property
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import service.ConfigService
import service.FileService
import utilities.ClusterID


class HiveClusterComponent(kodein: Kodein) : AbstractComponent(kodein) {

    @Property("global.basedir")
    val baseDir = "minidata"

    @Property("global.confDir")
    val confDir = "$baseDir/conf"


    @Property("global.secure")
    val secure = false

    @Property("hive.metastore.hostname")
    val hiveMetaStoreHostName = "127.0.0.1"

    @Property("hive.metastore.port")
    val hiveMetaStorePort = 11000

    @Property("hive.metastore.derbyDbDir")
    val hiveMetaStoreDerbyDbDir = "metastore_db"

    @Property("hive.metastore.scratchDir")
    val hiveMetaStoreScratchDir = "hive_scratch_dir"

    @Property("hive.metastore.warehouseDir")
    val hiveMetaStoreWarehouseDir = "hive_warehouse_dir"

    @Property("hive.server.hostname")
    val hiveServerHostname = "127.0.0.1"

    @Property("hive.server.port")
    val hiveServerPort = 11001
    @Property("zookeeper.connectionString")
    val zookeeperConnectionString = "127.0.0.1:6000"

    lateinit var hiveLocalServer2: HiveLocalServer2
    override fun launch(configuration: Configuration) {
        val hiveConf = configure(configuration) as HiveConf
        hiveLocalServer2 = HiveLocalServer2.Builder()
                .setHiveServer2Hostname(hiveServerHostname)
                .setHiveServer2Port(hiveServerPort)
                .setHiveMetastoreHostname(hiveMetaStoreHostName)
                .setHiveMetastorePort(hiveMetaStorePort)
                .setHiveMetastoreDerbyDbDir("$baseDir/$hiveMetaStoreDerbyDbDir")
                .setHiveScratchDir("$baseDir/$hiveMetaStoreScratchDir")
                .setHiveWarehouseDir("$baseDir/$hiveMetaStoreWarehouseDir")
                .setHiveConf(hiveConf)
                .setZookeeperConnectionString(zookeeperConnectionString)
                .build()

        hiveLocalServer2.start()
        writeConf(hiveLocalServer2.hiveConf)
    }

    override fun dependencies(): List<ClusterID> {
        val listOfDependencies = mutableListOf<ClusterID>(
                ClusterID.HIVEMETASTORE, ClusterID.ZOOKEEPER
        )
        if (secure) listOfDependencies.add(ClusterID.KDC)
        return listOfDependencies
    }

    override fun configuration(): Configuration {
        return hiveLocalServer2.hiveConf
    }

    override fun stop() {
        hiveLocalServer2.stop(true)//To change body of created functions use File | Settings | File Templates.
    }

    private fun configure(configuration: Configuration): Configuration {
        val hiveConf = HiveConf()
        hiveConf.addResource(configuration)
        return hiveConf
    }

    private fun writeConf(configuration: Configuration) {
        val configService: ConfigService = kodein.instance()
        configService.createConfFile(StringUtils.EMPTY, conf = configuration, outputFilePath = "$confDir/hive-site.xml")
    }


}