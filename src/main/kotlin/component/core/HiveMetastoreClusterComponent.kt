package component.core

import com.github.sakserv.minicluster.impl.HiveLocalMetaStore
import com.github.salomonbrys.kodein.Kodein
import com.github.salomonbrys.kodein.instance
import component.AbstractComponent
import info.macias.kaconf.Property
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import service.FileService
import utilities.ClusterID

class HiveMetastoreClusterComponent(kodein: Kodein) : AbstractComponent(kodein) {

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

    lateinit var hiveLocalMetaStore: HiveLocalMetaStore
    override fun launch(configuration: Configuration) {
        val hiveConf = HiveConf()
        hiveConf.addResource(configuration)
        hiveLocalMetaStore = HiveLocalMetaStore.Builder()
                .setHiveMetastoreHostname(hiveMetaStoreHostName)
                .setHiveMetastorePort(hiveMetaStorePort)
                .setHiveMetastoreDerbyDbDir("$baseDir/$hiveMetaStoreDerbyDbDir")
                .setHiveScratchDir("$baseDir/$hiveMetaStoreScratchDir")
                .setHiveWarehouseDir("$baseDir/$hiveMetaStoreWarehouseDir")
                .setHiveConf(hiveConf)
                .build()

        hiveLocalMetaStore.start()
    }

    override fun stop() {
        hiveLocalMetaStore.stop(true)//To change body of created functions use File | Settings | File Templates.
    }

    override fun clean() {
        val fileService: FileService = kodein.instance()
        fileService.delete("$baseDir/$hiveMetaStoreDerbyDbDir")
        fileService.delete("$baseDir/$hiveMetaStoreScratchDir")
        fileService.delete("$baseDir/$hiveMetaStoreWarehouseDir")
    }

    override fun dependencies(): List<ClusterID> {
        val listOfDependencies = mutableListOf<ClusterID>()
        if (secure) listOfDependencies.add(ClusterID.KDC)
        return listOfDependencies
    }

    override fun configuration(): Configuration {
        return hiveLocalMetaStore.hiveConf
    }

}