package component

import com.github.sakserv.minicluster.auth.Jaas
import com.github.salomonbrys.kodein.instance
import org.apache.hadoop.conf.Configuration
import service.JaasService


class SecuritySetupComponent : AbstractComponent<Jaas>() {

    override fun launch(configuration: Configuration): Jaas {
        val jaasService: JaasService by instance()
//        return jaasService.setJaas(
//                JaasService.JaasEntry(name = "Server", principal = getKrbPrincipal("zookeeper"), keytabPath = kdc.getKeytabForPrincipal("zookeeper"), service = "zookeeper"),
//                JaasService.JaasEntry(name = "Client", principal = kdc.getKrbPrincipal("zookeeper"), keytabPath = kdc.getKeytabForPrincipal("zookeeper")),
//                JaasService.JaasEntry(name = "KafkaServer", principal = kdc.getKrbPrincipal("kafka"), keytabPath = kdc.getKeytabForPrincipal("kafka"), service = "kafka"),
//                JaasService.JaasEntry(name = "KafkaClient", principal = kdc.getKrbPrincipal("local"), keytabPath = kdc.getKeytabForPrincipal("local"), service = "kafka")
//        )
        return Jaas()
    }
}