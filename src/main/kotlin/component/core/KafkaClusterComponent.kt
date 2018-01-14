package component.core

import com.github.sakserv.minicluster.impl.KafkaLocalBroker
import com.github.salomonbrys.kodein.Kodein
import com.github.salomonbrys.kodein.instance
import component.AbstractComponent
import component.Component
import info.macias.kaconf.Property
import org.apache.hadoop.conf.Configuration
import service.FileService
import utilities.ClusterID
import java.util.*

class KafkaClusterComponent(kodein: Kodein) : AbstractComponent(kodein) {

    @Property("global.basedir")
    val baseDir = "minidata"

    @Property("global.secure")
    val secure = false

    @Property("kerberos.tempDir")
    val kerberosTempDir = "embedded_kdc"

    @Property(value = "kafka.tempDir")
    val tempDir = "kafka_embedded"

    @Property("kafka.hostname")
    val kafkaHostname = "127.0.0.1"

    @Property("kafka.port")
    val kafkaPort = 9000

    @Property("kafka.sslPort")
    val kafkaSSLPort = 9096

    @Property("kafka.brokerId")
    val kafkaBrokerId = 1

    @Property("zookeeper.connectionString")
    val zookeeperConnectionString = "127.0.0.1:6000"

    @Property("ssl.keystore.password")
    val sskKeystorePassword = "serverP"

    @Property("ssl.key.password")
    val sslKeyPassword = "serverP"

    @Property("ssl.trustore.password")
    val sslTruststorePassword = "trustP"

    lateinit var kafkaLocalBroker: KafkaLocalBroker
    override fun launch(configuration: Configuration) {
        val properties = if (secure) prepareSecure() else Properties()
        kafkaLocalBroker = KafkaLocalBroker.Builder()
                .setKafkaHostname(kafkaHostname)
                .setKafkaPort(kafkaPort)
                .setKafkaBrokerId(kafkaBrokerId)
                .setKafkaProperties(properties)
                .setKafkaTempDir("$baseDir/$tempDir")
                .setZookeeperConnectionString(zookeeperConnectionString)
                .build()
        kafkaLocalBroker.start()
    }

    override fun dependencies(): List<ClusterID> {
        val listOfDependencies = mutableListOf<ClusterID>(
                ClusterID.ZOOKEEPER
        )
        if (secure) listOfDependencies.add(ClusterID.KDC)
        return listOfDependencies
    }

    override fun stop() {
        kafkaLocalBroker.stop(true)//To change body of created functions use File | Settings | File Templates.
    }

    override fun clean() {
        val fileService: FileService = kodein.instance()
        fileService.delete("$baseDir/$tempDir")
    }

    private fun prepareSecure(): Properties {
        val kdcDir = "$baseDir/$kerberosTempDir"
        val advertisedListeners = "PLAINTEXT://$kafkaHostname:$kafkaPort,SASL_SSL://$kafkaHostname:$kafkaSSLPort"
        val properties = Properties()
        properties.setProperty("ssl.truststore.location", "$kdcDir/trustKS.jks")
        properties.setProperty("ssl.keystore.location", "$kdcDir/serverKS.jks")
        properties.setProperty("advertised.listeners", advertisedListeners)
        properties.setProperty("listeners", advertisedListeners)
        properties.setProperty("security.inter.broker.protocol", "SASL_SSL")
        properties.setProperty("ssl.keystore.password", sskKeystorePassword)
        properties.setProperty("ssl.key.password", sslKeyPassword)
        properties.setProperty("ssl.truststore.password", sslTruststorePassword)
        properties.setProperty("sasl.kerberos.service.name", "kafka")
        return properties
    }



}