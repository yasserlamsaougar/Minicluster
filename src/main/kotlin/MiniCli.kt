import us.jimschubert.kopper.typed.BooleanArgument
import us.jimschubert.kopper.typed.StringArgument
import us.jimschubert.kopper.typed.TypedArgumentParser

class MiniCli(args: Array<String>) : TypedArgumentParser(args) {
    val componentsDefault = "hdfs,kafka,hbase,hive,yarn"

    val components by StringArgument(self, "c",
            default = componentsDefault,
            longOption = listOf("components"),
            description = "components to start [hdfs,yarn,hbase,kdc,hive,kafka,zookeeper] separated by ,")

    val configFile by StringArgument(self, "f",
            default = "",
            longOption = listOf("settings", "configuration"),
            description = "path to configuration file")

    val hooks by StringArgument(self, "g",
            default = "",
            longOption = listOf("hooks"),
            description = "list of packages containing the hooks to use separated by ,")

    val help by BooleanArgument(self, "h",
            default = true,
            longOption = listOf("help"),
            description = "show help")
}