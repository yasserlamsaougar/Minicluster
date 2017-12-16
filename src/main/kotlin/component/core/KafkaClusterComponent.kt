import com.github.sakserv.minicluster.impl.KafkaLocalBroker
import component.AbstractComponent
import component.Component
import info.macias.kaconf.Property
import org.apache.hadoop.conf.Configuration
import java.util.*

class KafkaClusterComponent : AbstractComponent<KafkaLocalBroker>() {

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

    override fun launch(configuration: Configuration) : KafkaLocalBroker {
        val properties = if(secure) prepareSecure() else Properties()
        val kafkaLocalBroker =  KafkaLocalBroker.Builder()
                .setKafkaHostname(kafkaHostname)
                .setKafkaPort(kafkaPort)
                .setKafkaBrokerId(kafkaBrokerId)
                .setKafkaProperties(properties)
                .setKafkaTempDir("$baseDir/$tempDir")
                .setZookeeperConnectionString(zookeeperConnectionString)
                .build()
        kafkaLocalBroker.start()
        return kafkaLocalBroker
    }


    fun prepareSecure() : Properties {
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