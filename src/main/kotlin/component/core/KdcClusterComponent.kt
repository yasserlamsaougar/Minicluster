package component.core

import clusters.KdcLocalClusterCorrected
import com.github.salomonbrys.kodein.Kodein
import com.github.salomonbrys.kodein.instance
import component.AbstractComponent
import info.macias.kaconf.Property
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import service.ConfigService
import service.FileService
import service.JaasService

class KdcClusterComponent(kodein: Kodein) : AbstractComponent(kodein) {

    @Property("global.basedir")
    val baseDir = "minidata"

    @Property("global.confDir")
    val confDir = "$baseDir/conf"

    @Property("global.secure")
    val secure = false

    @Property("kerberos.tempDir")
    val tempDir = "embedded_kdc"

    @Property("kerberos.port")
    val kerberosPort = 34000

    @Property("kerberos.host")
    val kerberosHost = "127.0.0.1"

    @Property("kerberos.orgDomain")
    val kerberosOrgDomain = "ORG"

    @Property("kerberos.instance")
    val kerberosInstance = "127.0.0.1"

    @Property("kerberos.transport")
    val kerberosTransport = "TCP"

    @Property("kerberos.maxTicketLifetime")
    val kerbeorsMaxTicketLifeTime = 86400000

    @Property("kerberos.maxRenewableLifetime")
    val kerberosMaxRenewableLifeTime = 604800000

    @Property("kerberos.debug")
    val kerberosDebug = false

    lateinit var kdcLocalCluster: KdcLocalClusterCorrected
    override fun launch(configuration: Configuration) {
        kdcLocalCluster = KdcLocalClusterCorrected.Builder()
                .setPort(kerberosPort)
                .setHost(kerberosHost)
                .setBaseDir("$baseDir/$tempDir")
                .setOrgDomain(kerberosOrgDomain)
                .setKrbInstance(kerberosInstance)
                .setInstance("DefaultKrbServer")
                .setTransport(kerberosTransport)
                .setMaxTicketLifetime(kerbeorsMaxTicketLifeTime)
                .setMaxRenewableLifetime(kerberosMaxRenewableLifeTime)
                .setDebug(kerberosDebug)
                .build()
        kdcLocalCluster.start()
        setSecure(kdcLocalCluster)
        setSecureConf(kdcLocalCluster)
        writeConf(kdcLocalCluster.baseConf!!)
    }

    private fun setSecure(kdc: KdcLocalClusterCorrected) {
        System.setProperty("java.security.krb5.conf", kdc.krb5conf.path)
        UserGroupInformation.setConfiguration(kdc.baseConf!!)

    }

    override fun configuration(): Configuration {
        return kdcLocalCluster.baseConf!!
    }

    override fun stop() {
        kdcLocalCluster.stop(true)//To change body of created functions use File | Settings | File Templates.
    }

    override fun clean() {
        val fileService: FileService = kodein.instance()
        fileService.delete("$baseDir/$tempDir")
    }

    private fun setSecureConf(kdc: KdcLocalClusterCorrected) {
        val jaasService: JaasService = kodein.instance()
        val globalJaas = jaasService.setJaas(
                JaasService.JaasEntry(name = "Server", principal = kdc.getKrbPrincipal("zookeeper"), keytabPath = kdc.getKeytabForPrincipal("zookeeper"), service = "zookeeper"),
                JaasService.JaasEntry(name = "Client", principal = kdc.getKrbPrincipal("zookeeper"), keytabPath = kdc.getKeytabForPrincipal("zookeeper")),
                JaasService.JaasEntry(name = "KafkaServer", principal = kdc.getKrbPrincipal("kafka"), keytabPath = kdc.getKeytabForPrincipal("kafka"), service = "kafka"),
                JaasService.JaasEntry(name = "KafkaClient", principal = kdc.getKrbPrincipal("local"), keytabPath = kdc.getKeytabForPrincipal("local"), service = "kafka")
        )
        val fileService: FileService = kodein.instance()
        fileService.writeStringToFile(jaasService.writeJaas(globalJaas), confDir + "/jaas.conf")
    }

    private fun writeConf(configuration: Configuration) {
        val configService: ConfigService = kodein.instance()
        val fileService:FileService = kodein.instance()
        configService.createConfFile(StringUtils.EMPTY, conf = configuration, outputFilePath = "$confDir/core-site.xml")
        fileService.copy(kdcLocalCluster.krb5conf.absolutePath, confDir)
        fileService.copyRegex("$baseDir/$tempDir", confDir, "keytab", "jks")
    }

}