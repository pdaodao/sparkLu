package top.myetl.lucenerdd.util

import java.io.{Closeable, File}
import java.util
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction
import org.apache.spark.{Logging, SparkConf}

/**
  * utils method with hdfs
  */
object FsUtils extends Logging{

  val FileSeparator: String = File.separator
  val HdfsFileSeparator: String = "/"
  val lock = new Object
  val FileSystems = new scala.collection.mutable.HashMap[String, (Int, FileSystem)]()

  def dirName(path: String*): String ={
    val paths = path.map( t => {
      if(t.endsWith(HdfsFileSeparator))
        t.substring(0, t.length-1)
      else
        t
    })
    paths.mkString(HdfsFileSeparator)
  }


  /**
    * list all file under this directory
    * @param fileSystem
    * @param path
    * @return
    */
  def listAll(fileSystem: FileSystem, path: Path): Array[String] = {
      val listStatus = fileSystem.listStatus(path)
      listStatus.map{ file => file.getPath.getName}
  }

  /**
    * List Lucene directory under this directory
    * @param fileSystem
    * @param path
    * @return
    */
  def listLuceneDir(fileSystem: FileSystem, path: Path): Array[String] = {
    val listStatus = fileSystem.listStatus(path)
    val paths = listStatus.filter(_.isDirectory).map(p => {
        p.getPath.getName
    })
    if(paths.length < 1 && listStatus.exists(_.getPath.getName.endsWith(".cfe"))) {
      return Array(path.getName)
    }
    paths
  }


  /**
    * hdfs base directory to store data , this directory must have write permission
    * @param conf
    * @return
    */
  def getHdfsBaseDir(conf: SparkConf): String = {
    val dirOption = conf.getOption(Constants.HdfsBaseDirKey)
    val sep = File.separator
    val dir = dirOption match {
      case d: Some[String] => if(d.get.endsWith(sep)) d.get else d.get+sep
      case _ => throw new IllegalArgumentException("hdfs base directory not set")
    }
    dir
  }

  /**
    * Get Hdfs file system
    * @param path
    * @param conf
    * @return
    */
  def get(path: Path, conf: Configuration): FileSystem = lock.synchronized{
    val dir = path.toUri.getPath
    FileSystem.get(path.toUri, conf)
//    val result: Option[(Int, FileSystem)] = FileSystems.get(dir)
//    result match {
//      case None =>
//        val fs = FileSystem.get(path.toUri, conf)
//        FileSystems.put(dir, (1, fs))
//        fs
//      case Some((times, fs)) =>
//        FileSystems.put(dir, (times+1, fs))
//        fs
//    }
  }

  def close(dir: String, fs: FileSystem): Unit = lock.synchronized{
    closeQuietly(fs)
//    val result: Option[(Int, FileSystem)] = FileSystems.get(dir)
//    result match {
//      case None => throw new RuntimeException("Try to close not exist fs system "+dir)
//      case Some((times, fs)) =>
//        if(times <= 1) {
//          println("close file system "+dir)
//          FileSystems.remove(dir)
//          closeQuietly(fs)
//        }
//        else FileSystems.put(dir, (times -1, fs))
//    }
  }

  /**
    * Get Hdfs file context
    * @param path
    * @param conf
    * @return
    */
  def getContext(path: Path, conf: Configuration): FileContext = {
    FileContext.getFileContext(path.toUri, conf)
  }

  /**
    * Make sure Hdfs is not in safe mode
    * @param fileSystem
    */
  def untilUnSafe(fileSystem: FileSystem): Unit = {
    if(fileSystem.isInstanceOf[DistributedFileSystem]){
      val fs = fileSystem.asInstanceOf[DistributedFileSystem]
      while(fs.setSafeMode(SafeModeAction.SAFEMODE_GET, true)){
        logWarning("The NameNode is in SafeMode wait 5 seconds and try again")
        try{
          Thread.sleep(5000)
        }catch {
          case e: InterruptedException => Thread.interrupted()
        }
      }
    }
  }

  /**
    * Make directory is not exists
    * @param path
    * @return
    */
  def mkDirIfNotExist(path: Path, fileSystem: FileSystem): Unit = {
    try{
      if(!fileSystem.exists(path)){
        val success = fileSystem.mkdirs(path)
        if(!success) throw new RuntimeException("Could not create directory: "+path)
        logDebug("create directory success "+path)
      }else{
        logDebug("directory already exists "+path)
      }
    }catch {
      case e: Exception => {
        close(path.toUri.getPath, fileSystem)
        throw new RuntimeException("Problem creating directory: " + path, e)
      }
    }
  }

  /**
    * Close the FileSystem
    * @param closeable
    */
  private def closeQuietly(closeable: Closeable ): Unit = {
    try {
      if (closeable != null) {
        closeable.close()
      }
    } catch {
      case e: Exception => logError("Error while closing", e)
    }
  }

  /**
    * Get hdfs OutputStream
    * @param fileSystem
    * @param path
    * @return
    */
  def getOutputStream(fileSystem: FileSystem, path: Path): FSDataOutputStream = {
    val conf: Configuration = fileSystem.getConf
    val fsDefaults: FsServerDefaults = fileSystem.getServerDefaults(path)
    val flags = util.EnumSet.of(CreateFlag.CREATE,
      CreateFlag.OVERWRITE, CreateFlag.SYNC_BLOCK)

    val permission = FsPermission.getDefault.applyUMask(FsPermission.getUMask(conf))
    fileSystem.create(path, permission, flags, fsDefaults.getFileBufferSize,
      fsDefaults.getReplication, fsDefaults.getBlockSize, null)
  }


}
