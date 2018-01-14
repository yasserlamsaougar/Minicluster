package service

import com.github.sakserv.minicluster.auth.Jaas
import com.github.sakserv.minicluster.auth.Jaas.NL

class JaasService {

    data class JaasEntry(val name: String, val principal: String, val keytabPath: String, val service: String = "")

    fun setJaas(vararg entries: JaasEntry, setAuth: Boolean = true): Jaas {
        val jaas = Jaas()
        entries.forEach { e ->
            if (!e.service.isEmpty()) {
                jaas.addServiceEntry(e.name, e.principal, e.keytabPath, e.service)
            } else {
                jaas.addEntry(e.name, e.principal, e.keytabPath)
            }
        }
        if (setAuth) javax.security.auth.login.Configuration.setConfiguration(jaas)
        return jaas
    }

    fun writeJaas(jaas: Jaas): String {
        return jaas.entries.map({ e ->
            val key = e.key
            val value = e.value
            val optionsAsString = value.options.map { option ->
                val optionKey = option.key
                val optionValue = option.value
                "\t$optionKey = \"$optionValue\""
            }.joinToString(separator = NL)
            "$key {\n\t${value.loginModuleName} required\n$optionsAsString\n;};"
        }).joinToString(separator = NL)
    }
}

