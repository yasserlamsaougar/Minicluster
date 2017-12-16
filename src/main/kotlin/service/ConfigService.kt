package service

import org.apache.hadoop.conf.Configuration
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*

class ConfigService {

    fun createConfFile(vararg  services: String, conf: Configuration, outputFilePath: String) {
        val configuration = Configuration(false)
        if (services.isNotEmpty()) {
            conf.getPropsWithPrefix("").filterKeys { k -> services.any{  s -> k.startsWith(s)} }.forEach { e ->
                configuration.set(e.key, e.value)
            }
            configuration.writeXml(Files.newOutputStream(Paths.get(outputFilePath)))
        } else {
            conf.writeXml(Files.newOutputStream(Paths.get(outputFilePath)))
        }
    }

}