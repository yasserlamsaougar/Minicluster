package component.core

import com.github.sakserv.minicluster.impl.HdfsLocalCluster
import com.github.salomonbrys.kodein.Kodein
import com.github.salomonbrys.kodein.instance
import component.AbstractComponent
import info.macias.kaconf.Property
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.HdfsConfiguration
import service.ConfigService
import service.FileService
import utilities.ClusterID

class HdfsClusterComponent(kodein: Kodein) : AbstractComponent(kodein) {

    @Property("global.basedir")
    val baseDir = "minidata"

    @Property("global.confDir")
    val confDir = "$baseDir/conf"

    @Property("global.secure")
    val secure = false

    @Property("hdfs.tempDir")
    val tempDir = "hdfs_embedded"

    @Property("hdfs.namenodePort")
    val hdfsNamenodePort: Int = 8800

    @Property("hdfs.namenodeHttpPort")
    val hdfsNamenodeHttpPort = 8832

    @Property("hdfs.numDatanodes")
    val hdfsNumDatanodes = 1

    @Property("hdfs.enablePermissions")
    val hdfsEnablePermissions = false

    @Property("hdfsFormat")
    val hdfsFormat = true

    @Property("hdfs.enableRunningUserAsProxyUser")
    val hdfsEnableRunningUserAsProxyUser = true

    private lateinit var hdfsLocalCluster:HdfsLocalCluster
    override fun launch(configuration: Configuration) {
        val hdfsConfiguration = configure(configuration)
        hdfsLocalCluster =  HdfsLocalCluster.Builder()
                .setHdfsNamenodePort(hdfsNamenodePort)
                .setHdfsNamenodeHttpPort(hdfsNamenodeHttpPort)
                .setHdfsTempDir("$baseDir/$tempDir")
                .setHdfsNumDatanodes(hdfsNumDatanodes)
                .setHdfsEnablePermissions(hdfsEnablePermissions)
                .setHdfsFormat(hdfsFormat)
                .setHdfsEnableRunningUserAsProxyUser(hdfsEnableRunningUserAsProxyUser)
                .setHdfsConfig(hdfsConfiguration)
                .build()
        hdfsLocalCluster.start()
        writeConf(hdfsLocalCluster.hdfsConfig)
    }

    override fun configuration(): Configuration {
        return hdfsLocalCluster.hdfsConfig
    }

    override fun dependencies(): List<ClusterID> {
        val listOfDependencies = mutableListOf<ClusterID>()
        if (secure) listOfDependencies.add(ClusterID.KDC)
        return listOfDependencies
    }

    override fun stop() {
        hdfsLocalCluster.stop(true)//To change body of created functions use File | Settings | File Templates.
    }

    override fun clean() {
        val fileService: FileService = kodein.instance()
        fileService.delete("$baseDir/$tempDir")
    }

    private fun configure(configuration: Configuration) : Configuration{
        val hdfsConfiguration = HdfsConfiguration()
        hdfsConfiguration.set("fs.defaultFS", "hdfs://127.0.0.1:$hdfsNamenodePort")
        hdfsConfiguration.addResource(configuration)
        return hdfsConfiguration
    }

    private fun writeConf(configuration: Configuration) {
        val configService: ConfigService = kodein.instance()
        configService.createConfFile(StringUtils.EMPTY, conf = configuration, outputFilePath = "$confDir/hdfs-site.xml")
    }


}