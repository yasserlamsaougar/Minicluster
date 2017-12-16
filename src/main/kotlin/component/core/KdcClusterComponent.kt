package component

import clusters.KdcLocalClusterCorrected
import info.macias.kaconf.Property
import org.apache.hadoop.conf.Configuration

class KdcClusterComponent : AbstractComponent<KdcLocalClusterCorrected>() {

    @Property("global.basedir")
    val baseDir = "minidata"

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

    override fun launch(configuration: Configuration) : KdcLocalClusterCorrected {
        val kdcLocalCluster = KdcLocalClusterCorrected.Builder()
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
        return kdcLocalCluster
    }
}