import com.github.sakserv.minicluster.impl.HdfsLocalCluster
import component.AbstractComponent
import component.Component
import info.macias.kaconf.Property
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.HdfsConfiguration

class HdfsClusterComponent : AbstractComponent<HdfsLocalCluster>() {

    @Property("global.basedir")
    val baseDir = "minidata"

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

    override fun launch(configuration: Configuration) : HdfsLocalCluster {
        val hdfsConfiguration = configure(configuration)
        val hdfsLocalCluster =  HdfsLocalCluster.Builder()
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
        return hdfsLocalCluster
    }

    private fun configure(configuration: Configuration) : Configuration{
        val hdfsConfiguration = HdfsConfiguration()
        hdfsConfiguration.set("fs.default.name", "hdfs://127.0.0.1:$hdfsNamenodePort")
        hdfsConfiguration.addResource(configuration)
        return hdfsConfiguration
    }


}