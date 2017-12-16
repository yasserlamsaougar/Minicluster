package clusters/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


import com.github.sakserv.minicluster.MiniCluster
import com.github.sakserv.minicluster.util.FileUtils
import com.github.sakserv.minicluster.util.WindowsLibsUtils
import com.google.common.base.Throwables
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeys
import org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY
import org.apache.hadoop.fs.CommonConfigurationKeysPublic
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.DFSConfigKeys.*
import org.apache.hadoop.http.HttpConfig
import org.apache.hadoop.minikdc.MiniKdc
import org.apache.hadoop.security.SecurityUtil
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.authentication.util.KerberosUtil
import org.apache.hadoop.security.ssl.KeyStoreTestUtil
import org.slf4j.LoggerFactory
import java.io.File
import java.util.*

class KdcLocalClusterCorrected private constructor(builder: Builder) : MiniCluster {

    private var miniKdc: MiniKdc? = null
    private val orgName: String?
    private val orgDomain: String?
    private val port: Int?
    private val host: String?
    private val baseDir: String?
    private val krbInstance: String?
    private var principals: List<String>? = null
    private val instance: String?
    private val transport: String?
    private val maxTicketLifetime: Int?
    private val maxRenewableLifetime: Int?
    private val debug: Boolean?

    var baseConf: Configuration? = null
    private var conf: Properties? = null

    val krb5conf: File
        get() = miniKdc!!.krb5conf

    val realm: String
        get() = miniKdc!!.realm

    init {
        this.orgName = builder.orgName
        this.orgDomain = builder.orgDomain
        this.port = builder.port
        this.host = builder.host
        this.baseDir = builder.baseDir
        this.krbInstance = builder.krbInstance
        this.principals = builder.principals
        this.instance = builder.instance
        this.transport = builder.transport
        this.maxTicketLifetime = builder.maxTicketLifetime
        this.maxRenewableLifetime = builder.maxRenewableLifetime
        this.debug = builder.debug
    }

    class Builder {
        var orgName = "acme"
        var orgDomain = "org"
        var port: Int? = null
        var host: String? = null
        var baseDir: String? = null
        var krbInstance = if (Path.WINDOWS) "127.0.0.1" else "localhost"
        var principals = DEFAULT_PRINCIPALS
        var instance = "DefaultKrbServer"
        var transport = "TCP"
        var maxTicketLifetime: Int? = 86400000
        var maxRenewableLifetime: Int? = 604800000
        var debug: Boolean? = false

        fun setOrgName(orgName: String): Builder {
            this.orgName = orgName
            return this
        }

        fun setOrgDomain(orgDomain: String): Builder {
            this.orgDomain = orgDomain
            return this
        }

        fun setPort(port: Int?): Builder {
            this.port = port
            return this
        }

        fun setHost(host: String): Builder {
            this.host = host
            return this
        }

        fun setBaseDir(baseDir: String): Builder {
            this.baseDir = baseDir
            return this
        }

        fun setKrbInstance(krbInstance: String): Builder {
            this.krbInstance = krbInstance
            return this
        }

        fun setPrincipals(principals: Array<String>): Builder {
            this.principals = Arrays.asList(*principals)
            return this
        }

        fun setInstance(instance: String): Builder {
            this.instance = instance
            return this
        }

        fun setTransport(transport: String): Builder {
            this.transport = transport
            return this
        }

        fun setMaxTicketLifetime(maxTicketLifetime: Int?): Builder {
            this.maxTicketLifetime = maxTicketLifetime
            return this
        }

        fun setMaxRenewableLifetime(maxRenewableLifetime: Int?): Builder {
            this.maxRenewableLifetime = maxRenewableLifetime
            return this
        }

        fun setDebug(debug: Boolean?): Builder {
            this.debug = debug
            return this
        }

        fun build(): KdcLocalClusterCorrected {
            val kdcLocalCluster = KdcLocalClusterCorrected(this)
            validateObject(kdcLocalCluster)
            return kdcLocalCluster
        }

        private fun validateObject(kdcLocalCluster: KdcLocalClusterCorrected) {
            if (kdcLocalCluster.orgName == null) {
                throw IllegalArgumentException("ERROR: Missing required config: KDC Name")
            }
            if (kdcLocalCluster.orgDomain == null) {
                throw IllegalArgumentException("ERROR: Missing required config: KDC Domain")
            }
            if (kdcLocalCluster.host == null) {
                throw IllegalArgumentException("ERROR: Missing required config: KDC Host")
            }
            if (kdcLocalCluster.baseDir == null) {
                throw IllegalArgumentException("ERROR: Missing required config: KDC BaseDir")
            }
            if (kdcLocalCluster.krbInstance == null) {
                throw IllegalArgumentException("ERROR: Missing required config: KDC KrbInstance")
            }
            if (CollectionUtils.isEmpty(kdcLocalCluster.principals)) {
                throw IllegalArgumentException("ERROR: Missing required config: KDC Principals")
            }
            if (kdcLocalCluster.instance == null) {
                throw IllegalArgumentException("ERROR: Missing required config: KDC Instance")
            }
            if (kdcLocalCluster.transport == null) {
                throw IllegalArgumentException("ERROR: Missing required config: KDC Tranport")
            }
            if (kdcLocalCluster.maxTicketLifetime == null) {
                throw IllegalArgumentException("ERROR: Missing required config: KDC MaxTicketLifetime")
            }
            if (kdcLocalCluster.maxRenewableLifetime == null) {
                throw IllegalArgumentException("ERROR: Missing required config: KDC MaxRenewableLifetime")
            }
            if (kdcLocalCluster.debug == null) {
                throw IllegalArgumentException("ERROR: Missing required config: KDC Debug")
            }
        }
    }

    fun getKrbPrincipalWithRealm(principal: String): String {
        return "$principal/$krbInstance@" + miniKdc!!
                .realm
    }

    fun getKrbPrincipal(principal: String): String {
        return principal + "/" + krbInstance
    }

    fun getKeytabForPrincipal(principal: String): String {
        return FilenameUtils.separatorsToUnix(File(baseDir, principal + ".keytab").absolutePath)
    }


    @Throws(Exception::class)
    override fun start() {

        LOG.info("KDC: Starting MiniKdc")
        configure()
        miniKdc = MiniKdc(conf!!, File(baseDir!!))
        miniKdc!!.start()

        val ugi = UserGroupInformation.createRemoteUser("guest")
        UserGroupInformation.setLoginUser(ugi)
        val username = UserGroupInformation.getLoginUser().shortUserName

        val temp = ArrayList(principals!!)
        temp.add(username)
        this.principals = Collections.unmodifiableList(temp)

        principals!!.distinct().forEach { p ->
            try {
                val keytab = File(baseDir, p + ".keytab")
                LOG.info("KDC: Creating keytab for {} in {}", p, keytab)
                miniKdc!!.createPrincipal(keytab, p, getKrbPrincipal(p), getKrbPrincipalWithRealm(p))
            } catch (e: Exception) {
                throw Throwables.propagate(e)
            }
        }
        refreshDefaultRealm()
        prepareSecureConfiguration(username)
    }

    @Throws(Exception::class)
    private fun refreshDefaultRealm() {
        // Config is statically initialized at this point. But the above configuration results in a different
        // initialization which causes the tests to fail. So the following two changes are required.

        // (1) Refresh Kerberos config.
        // refresh the config
        val configClass = if (System.getProperty("java.vendor").contains("IBM")) {
            Class.forName("com.ibm.security.krb5.internal.Config")
        } else {
            Class.forName("sun.security.krb5.Config")
        }
        val refreshMethod = configClass.getMethod("refresh", *arrayOfNulls(0))
        refreshMethod.invoke(configClass, *arrayOfNulls(0))
        // (2) Reset the default realm.
        try {
            val hadoopAuthClass = Class.forName("org.apache.hadoop.security.authentication.util.KerberosName")
            val defaultRealm = hadoopAuthClass.getDeclaredField("defaultRealm")
            defaultRealm.isAccessible = true
            defaultRealm.set(null, KerberosUtil.getDefaultRealm())
            LOG.info("HADOOP: Using default realm " + KerberosUtil.getDefaultRealm())
        } catch (e: ClassNotFoundException) {
            // Don't care
            LOG.info("Class org.apache.hadoop.security.authentication.util.KerberosName not found, Kerberos default realm not updated")
        }

        try {
            val zookeeperAuthClass = Class.forName("org.apache.zookeeper.server.auth.KerberosName")
            val defaultRealm = zookeeperAuthClass.getDeclaredField("defaultRealm")
            defaultRealm.isAccessible = true
            defaultRealm.set(null, KerberosUtil.getDefaultRealm())
            LOG.info("ZOOKEEPER: Using default realm " + KerberosUtil.getDefaultRealm())
        } catch (e: ClassNotFoundException) {
            // Don't care
            LOG.info("Class org.apache.zookeeper.server.auth.KerberosName not found, Kerberos default realm not updated")
        }

    }

    @Throws(Exception::class)
    private fun prepareSecureConfiguration(username: String) {
        baseConf = Configuration(false)
        SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, baseConf!!)
        baseConf!!.setBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, true)
        //baseConf.set(CommonConfigurationKeys.HADOOP_RPC_PROTECTION, "authentication");

        val sslConfigDir = KeyStoreTestUtil.getClasspathDir(this.javaClass)
        KeyStoreTestUtil.setupSSLConfig(baseDir, sslConfigDir, baseConf!!, false)

        // User
        baseConf!!.set("hadoop.proxyuser.$username.hosts", "*")
        baseConf!!.set("hadoop.proxyuser.$username.groups", "*")

        // HTTP
        val spnegoPrincipal = getKrbPrincipalWithRealm(SPNEGO_USER_NAME)
        baseConf!!.set("hadoop.proxyuser.${SPNEGO_USER_NAME}.groups", "*")
        baseConf!!.set("hadoop.proxyuser.${SPNEGO_USER_NAME}.hosts", "*")

        // Oozie
        val ooziePrincipal = getKrbPrincipalWithRealm(OOZIE_USER_NAME)
        baseConf!!.set("hadoop.proxyuser.${OOZIE_USER_NAME}.hosts", "*")
        baseConf!!.set("hadoop.proxyuser.${OOZIE_USER_NAME}.groups", "*")
        baseConf!!.set("hadoop.user.group.static.mapping.overrides", OOZIE_PROXIED_USER_NAME + "=oozie")
        baseConf!!.set("oozie.service.HadoopAccessorService.keytab.file", getKeytabForPrincipal(OOZIE_USER_NAME))
        baseConf!!.set("oozie.service.HadoopAccessorService.kerberos.principal", ooziePrincipal)
        baseConf!!.setBoolean("oozie.service.HadoopAccessorService.kerberos.enabled", true)

        // HDFS
        val hdfsPrincipal = getKrbPrincipalWithRealm(HDFS_USER_NAME)
        baseConf!!.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal)
        baseConf!!.set(DFS_NAMENODE_KEYTAB_FILE_KEY, getKeytabForPrincipal(HDFS_USER_NAME))
        baseConf!!.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal)
        baseConf!!.set(DFS_DATANODE_KEYTAB_FILE_KEY, getKeytabForPrincipal(HDFS_USER_NAME))
        baseConf!!.set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, spnegoPrincipal)
        baseConf!!.set(DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY, getKeytabForPrincipal(SPNEGO_USER_NAME))
        baseConf!!.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true)
        baseConf!!.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication")
        baseConf!!.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name)
        baseConf!!.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0")
        baseConf!!.set(DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0")
        baseConf!!.set(DFS_JOURNALNODE_HTTPS_ADDRESS_KEY, "localhost:0")
        baseConf!!.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY, 10)
        // Kafka
        baseConf!!.set("hadoop.proxyuser.${HDFS_USER_NAME}.groups", "*")
        baseConf!!.set("hadoop.proxyuser.${HDFS_USER_NAME}.hosts", "*")
        // HBase
        val hbasePrincipal = getKrbPrincipalWithRealm(HBASE_USER_NAME)
        baseConf!!.set("hbase.security.authentication", "kerberos")
        baseConf!!.setBoolean("hbase.security.authorization", true)
        baseConf!!.set("hbase.regionserver.kerberos.principal", hbasePrincipal)
        baseConf!!.set("hbase.regionserver.keytab.file", getKeytabForPrincipal(HBASE_USER_NAME))
        baseConf!!.set("hbase.master.kerberos.principal", hbasePrincipal)
        baseConf!!.set("hbase.master.keytab.file", getKeytabForPrincipal(HBASE_USER_NAME))
        baseConf!!.set("hbase.coprocessor.region.classes", "org.apache.hadoop.hbase.security.token.TokenProvider")
        baseConf!!.set("hbase.rest.authentication.kerberos.keytab", getKeytabForPrincipal(SPNEGO_USER_NAME))
        baseConf!!.set("hbase.rest.authentication.kerberos.principal", spnegoPrincipal)
        baseConf!!.set("hbase.rest.kerberos.principal", hbasePrincipal)
        baseConf!!.set("hbase.zookeeper.client.keytab.file", getKeytabForPrincipal(ZOOKEEPER_USER_NAME))
        baseConf!!.set("hadoop.proxyuser.${HBASE_USER_NAME}.groups", "*")
        baseConf!!.set("hadoop.proxyuser.${HBASE_USER_NAME}.hosts", "*")

        //hbase.coprocessor.master.classes -> org.apache.hadoop.hbase.security.access.AccessController
        //hbase.coprocessor.region.classes -> org.apache.hadoop.hbase.security.token.TokenProvider,org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint,org.apache.hadoop.hbase.security.access.AccessController

        // Storm
        //String stormPrincipal = getKrbPrincipalWithRealm(STORM_USER_NAME);

        // Yarn
        val yarnPrincipal = getKrbPrincipalWithRealm(YARN_USER_NAME)
        baseConf!!.set("yarn.resourcemanager.keytab", getKeytabForPrincipal(YARN_USER_NAME))
        baseConf!!.set("yarn.resourcemanager.principal", yarnPrincipal)
        baseConf!!.set("yarn.nodemanager.keytab", getKeytabForPrincipal(YARN_USER_NAME))
        baseConf!!.set("yarn.nodemanager.principal", yarnPrincipal)
        baseConf!!.set("yarn.timeline-service.keytab", getKeytabForPrincipal(YARN_USER_NAME))
        baseConf!!.set("yarn.timeline-service.principal", yarnPrincipal)
        // Mapreduce
        val mrv2Principal = getKrbPrincipalWithRealm(MRV2_USER_NAME)
        baseConf!!.set("mapreduce.jobhistory.keytab", getKeytabForPrincipal(MRV2_USER_NAME))
        baseConf!!.set("mapreduce.jobhistory.principal", mrv2Principal)

        // Kafka
        baseConf!!.set("hadoop.proxyuser.${KAFKA_USER_NAME}.groups", "*")
        baseConf!!.set("hadoop.proxyuser.${KAFKA_USER_NAME}.hosts", "*")

        // hive
        val hivePrincipal = getKrbPrincipalWithRealm(HIVE_USER_NAME)
        baseConf!!.set("hive.server2.authentification", "KERBEROS")
        baseConf!!.set("hive.server2.authentication.kerberos.principal", hivePrincipal)
        baseConf!!.set("hive.server2.authentication.kerberos.keytab", getKeytabForPrincipal(HIVE_USER_NAME))

        baseConf!!.set("hive.metastore.sasl.enabled", "true")
        baseConf!!.set("hive.metastore.kerberos.keytab.file", getKeytabForPrincipal(HIVE_USER_NAME))
        baseConf!!.set("hive.metastore.kerberos.principal", hivePrincipal)

    }

    @Throws(Exception::class)
    override fun stop() {
        stop(true)
    }

    @Throws(Exception::class)
    override fun stop(cleanUp: Boolean) {
        LOG.info("KDC: Stopping MiniKdc")
        miniKdc!!.stop()
        if (cleanUp) {
            cleanUp()
        }
    }

    @Throws(Exception::class)
    override fun configure() {
        conf = Properties()
        conf!!.setProperty("kdc.port", Integer.toString(port!!))
        conf!!.setProperty("kdc.bind.address", host)
        conf!!.setProperty("org.name", orgName)
        conf!!.setProperty("org.domain", orgDomain)
        conf!!.setProperty("instance", instance)
        conf!!.setProperty("transport", transport)
        conf!!.setProperty("max.ticket.lifetime", Integer.toString(maxTicketLifetime!!))
        conf!!.setProperty("max.renewable.lifetime", Integer.toString(maxRenewableLifetime!!))
        conf!!.setProperty("debug", java.lang.Boolean.toString(debug!!))

        // Handle Windows
        WindowsLibsUtils.setHadoopHome()
    }

    @Throws(Exception::class)
    override fun cleanUp() {
        FileUtils.deleteFolder(baseDir, true)
    }

    companion object {

        // Logger
        private val LOG = LoggerFactory.getLogger(KdcLocalClusterCorrected::class.java)

        private val HDFS_USER_NAME = "local"
        private val HBASE_USER_NAME = "local"
        private val YARN_USER_NAME = "local"
        private val MRV2_USER_NAME = "local"
        private val ZOOKEEPER_USER_NAME = "zookeeper"
        private val STORM_USER_NAME = "local"
        private val OOZIE_USER_NAME = "local"
        private val OOZIE_PROXIED_USER_NAME = "local"
        private val SPNEGO_USER_NAME = "local"
        private val KAFKA_USER_NAME = "kafka"
        private val HIVE_USER_NAME = "local"

        private var DEFAULT_PRINCIPALS = Collections.unmodifiableList(Arrays.asList(
                HDFS_USER_NAME, HBASE_USER_NAME, YARN_USER_NAME, MRV2_USER_NAME, ZOOKEEPER_USER_NAME, STORM_USER_NAME, OOZIE_USER_NAME, OOZIE_PROXIED_USER_NAME, SPNEGO_USER_NAME, KAFKA_USER_NAME, HIVE_USER_NAME
        ))
    }
}
