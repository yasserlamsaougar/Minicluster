package component

import com.github.sakserv.minicluster.impl.HiveLocalMetaStore
import info.macias.kaconf.Property
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf

class HiveMetaStoreComponent : AbstractComponent<HiveLocalMetaStore>(){

    @Property("global.basedir")
    val baseDir = "minidata"

    @Property("global.secure")
    val secure = false

    @Property("hive.metastore.hostname")
    val hiveMetaStoreHostName = "127.0.0.1"

    @Property("hive.metastore.port")
    val hiveMetaStorePort =  11000

    @Property("hive.metastore.derbyDbDir")
    val hiveMetaStoreDerbyDbDir = "metastore_db"

    @Property("hive.metastore.scratchDir")
    val hiveMetaStoreScratchDir = "hive_scratch_dir"

    @Property("hive.metastore.warehouseDir")
    val hiveMetaStoreWarehouseDir = "hive_warehouse_dir"

    override fun launch(configuration: Configuration) : HiveLocalMetaStore {
        val hiveConf = HiveConf()
        hiveConf.addResource(configuration)
        val hiveLocalMetaStore = HiveLocalMetaStore.Builder()
                .setHiveMetastoreHostname(hiveMetaStoreHostName)
                .setHiveMetastorePort(hiveMetaStorePort)
                .setHiveMetastoreDerbyDbDir("$baseDir/$hiveMetaStoreDerbyDbDir")
                .setHiveScratchDir("$baseDir/$hiveMetaStoreScratchDir")
                .setHiveWarehouseDir("$baseDir/$hiveMetaStoreWarehouseDir")
                .setHiveConf(hiveConf)
                .build()

        hiveLocalMetaStore.start()

        return hiveLocalMetaStore
    }

}