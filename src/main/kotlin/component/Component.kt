package component

import org.apache.hadoop.conf.Configuration
import utilities.ClusterID

interface Component {

    fun launch(configuration: Configuration = Configuration())

    fun stop()

    fun clean() {}

    fun dependencies() : List<ClusterID> {
        return emptyList()
    }

    fun configuration() : Configuration{
        return Configuration()
    }



}