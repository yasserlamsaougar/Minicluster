package service

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import java.io.File
import java.nio.file.Paths

class FileService {

    fun copy(input: String, output: String, isFile:Boolean = false) {
        val inputPath = Paths.get(input)
        val outputPath = if (!isFile) Paths.get("$output/${inputPath.fileName}") else Paths.get("$output/")
        FileUtils.copyFile(inputPath.toFile(), outputPath.toFile())
    }

    fun copyRegex(input: String, output: String, vararg extensions: String) {
        FileUtils.listFiles(File(input), extensions, false).forEach { f ->
            copy(f.absolutePath, output)
        }
    }

    fun writeStringToFile(text: String, outputPath: String) {
        FileUtils.writeStringToFile(Paths.get(outputPath).toFile(), text)
    }

    fun delete(path: String) {
        FileUtils.deleteQuietly(File(path))
    }

    fun createDirRecu(dir: String) {
        FileUtils.forceMkdir(File(dir))
    }

}