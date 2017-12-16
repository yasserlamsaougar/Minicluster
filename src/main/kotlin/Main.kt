import component.*
import info.macias.kaconf.ConfiguratorBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import service.ConfigService
import service.FileService
import service.JaasService
import java.util.*


fun main(args: Array<String>) {

    System.setProperty("sun.security.krb5.debug", "true")
    val properties = Properties()
    properties.load(ClassLoader.getSystemClassLoader().getResourceAsStream("cluster.properties"))
    val conf = ConfiguratorBuilder()
            .addSource(System.getenv())
            .addSource(System.getProperties())
            .addSource(properties).build()

    val hdfsComponent = HdfsClusterComponent()
    val cleanComponent = CleanSetupComponent()
    val securityComponent = SecuritySetupComponent()
    val hbaseComponent = HbaseClusterComponent()
    val zookeeperComponent = ZookeeperClusterComponent()
    val kerberosComponent = KdcClusterComponent()
    val kafkaComponent = KafkaClusterComponent()
    val yarnComponent = YarnClusterComponent()
    val hiveMetastoreComponent = HiveMetaStoreComponent()
    val hiveComponent = HiveClusterComponent()

    conf.configure(hdfsComponent)
    conf.configure(cleanComponent)
    conf.configure(hbaseComponent)
    conf.configure(zookeeperComponent)
    conf.configure(kerberosComponent)
    conf.configure(kafkaComponent)
    conf.configure(yarnComponent)
    conf.configure(hiveMetastoreComponent)
    conf.configure(hiveComponent)
    conf.configure(securityComponent)
    val kdc = kerberosComponent.launch()

    // setting security
    System.setProperty("java.security.krb5.conf'",  kdc.krb5conf.path)
    UserGroupInformation.setConfiguration(kdc.baseConf!!)
    // launching kafka cluster
    securityComponent.launch()

    // launching zookeeper
    zookeeperComponent.launch()

    // launching hdfs cluster
    val hdfsLocalCluster = hdfsComponent.launch(kdc.baseConf!!)
    UserGroupInformation.loginUserFromKeytab(kdc.getKrbPrincipal("local"), kdc.getKeytabForPrincipal("local"))

    // launching yarn cluster
    yarnComponent.launch(hdfsLocalCluster.hdfsConfig)

    // launching kafka broker
    kafkaComponent.launch()

    // launch hbase cluster
    hbaseComponent.launch(kdc.baseConf!!)

    val hiveMetastoreLocal = hiveMetastoreComponent.launch(kdc.baseConf!!)
    hiveComponent.launch(hiveMetastoreLocal.hiveConf)

//    val configuration = Configuration()
//    configuration.addResource(hbaseLocalCluster.hbaseConfiguration)
//    configuration.addResource(hdfsLocalCluster.hdfsConfig)
//    configuration.addResource(yarnLocalCluster.config)
//    configuration.addResource(hiveMetastoreLocal.hiveConf)
//    configuration.addResource(hiveLocalCluster.hiveConf)
//
//    val confDir = properties.getProperty("global.confDir")
//    val libsDir = properties.getProperty("global.libsDir")
//
//    FileService.createDirRecu(confDir)
//    FileService.createDirRecu(libsDir)
//
//    ConfigService.createConfFile("dfs", "hadoop", "fs", conf = hdfsLocalCluster.hdfsConfig, outputFilePath = "$confDir/hdfs-site.xml")
//    ConfigService.createConfFile("yarn", "dfs", "hadoop", "spark", "fs", conf = yarnLocalCluster.config, outputFilePath = "$confDir/yarn-site.xml")
//    ConfigService.createConfFile("hbase", "zookeeper", conf = hbaseLocalCluster.hbaseConfiguration, outputFilePath = "$confDir/hbase-site.xml")
//    ConfigService.createConfFile("hadoop", "dfs", "fs", "ipc", conf = configuration, outputFilePath = "$confDir/core-site.xml")
//    ConfigService.createConfFile("hive", "fs", conf = hiveLocalCluster.hiveConf, outputFilePath = "$confDir/hive-site.xml")
//    ConfigService.createConfFile("", conf = configuration, outputFilePath = "$confDir/all-site.xml")
//
//    FileService.copyFile(kdc.krb5conf.path, confDir)
//    FileService.copyFile("${kdc.krb5conf.parent}/../local.keytab", confDir)
//    FileService.copyFile("${kdc.krb5conf.parent}/../trustKS.jks", "$confDir/truststore", true)
//    FileService.writeStringToFile(text = JaasService.writeJaas(globalJaas), outputPath = "$confDir/kafka-jaas.conf")
//
//
//    val fs = hdfsLocalCluster.hdfsFileSystemHandle
//    //  uploading specific components to hdfs
//    FileService.writeFileToHdfs(fs, confDir, "hdfs:///user/dco_app/usr/conf", hdfsLocalCluster.hdfsConfig)
//    FileService.writeFileToHdfs(fs, confDir, "hdfs:///user/dco_app_edma/usr/conf", hdfsLocalCluster.hdfsConfig)
//    FileService.writeFileToHdfs(fs, libsDir, "hdfs:///user/dco_app/usr/lib", hdfsLocalCluster.hdfsConfig)
//    FileService.writeFileToHdfs(fs, libsDir, "hdfs:///user/dco_app_edma/usr/lib", hdfsLocalCluster.hdfsConfig)


}
