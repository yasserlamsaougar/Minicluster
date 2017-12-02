import com.github.sakserv.minicluster.impl.HdfsLocalCluster
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext


fun main(args: Array<String>) {
    val hdfsLocalCluster = HdfsLocalCluster.Builder()
        .setHdfsNamenodePort(12345)
        .setHdfsNamenodeHttpPort(12341)
        .setHdfsTempDir("embedded_hdfs")
        .setHdfsNumDatanodes(1)
        .setHdfsEnablePermissions(false)
        .setHdfsFormat(true)
        .setHdfsEnableRunningUserAsProxyUser(true)
        .setHdfsConfig(Configuration())
        .build()

    hdfsLocalCluster.start()

    val conf = SparkConf().setAppName("SparkJoins").setMaster("local")
    val context = JavaSparkContext.fromSparkContext(SparkContext(conf))

    val rdd = context.parallelize(intArrayOf(1, 2, 3).asList())
        rdd.saveAsTextFile("hdfs://localhost:12345/test.log")

}
