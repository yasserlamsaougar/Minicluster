package service

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import java.io.File

class HdfsFileService {

    fun writeFileToHdfs(fs: FileSystem, localDir: String, hdfsDir: String, config: Configuration = Configuration()) {

        FileUtil.copy(File(localDir), fs, Path(hdfsDir), false, config)
    }
}