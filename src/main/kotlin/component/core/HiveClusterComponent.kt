package component

import com.github.sakserv.minicluster.impl.HiveLocalServer2
import info.macias.kaconf.Property
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf


class HiveClusterComponent : AbstractComponent<HiveLocalServer2>() {

    @Property("global.basedir")
    val baseDir = "minidata"

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

    override fun launch(configuration: Configuration): HiveLocalServer2 {
        val hiveConf = configure(configuration) as HiveConf
        val hiveLocalServer2 = HiveLocalServer2.Builder()
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
        return hiveLocalServer2
    }

    private fun configure(configuration: Configuration) : Configuration {
        val hiveConf = HiveConf()
        hiveConf.addResource(configuration)
        return hiveConf
    }


}