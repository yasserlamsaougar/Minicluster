package component

import com.github.sakserv.minicluster.MiniCluster
import com.github.salomonbrys.kodein.KodeinInjected
import com.github.salomonbrys.kodein.KodeinInjector
import org.apache.hadoop.conf.Configuration

interface Component<out T> : KodeinInjected {

    fun launch(configuration: Configuration = Configuration()) : T


}