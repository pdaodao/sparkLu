package top.myetl.lucenerdd.store

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, FileStatus, FileSystem, Path}
import org.apache.lucene.store._
import org.apache.spark.Logging
import top.myetl.lucenerdd.util.FsUtils
/**
  * Hdfs Directory
  */
class HdfsDirectory(val path: Path, lockFactory: LockFactory,
                    val config: Configuration, val bufferSize: Int)
  extends BaseDirectory(lockFactory) with Logging{
  import HdfsDirectory._

  val dir = path.toUri.getPath

  def fileSystem: FileSystem = FsUtils.get(path, config)

  val fileContext: FileContext = FsUtils.getContext(path, config)

  FsUtils.untilUnSafe(fileSystem)
  FsUtils.mkDirIfNotExist(path, fileSystem)

  /**
    * Close the Direcoty
    */
  override def close(): Unit = {
    if(fileSystem != null && isOpen){
      logInfo("Closing hdfs direcoty: "+path)
      FsUtils.close(dir, fileSystem)
      isOpen = false
    }
  }

  def isClosed(): Boolean = !isOpen

  /**
    * Write the file to the Hdfs
    * @param name
    * @param context
    * @return
    */
  override def createOutput(name: String, context: IOContext): IndexOutput = {
    HdfsIndexOut(fileSystem, new Path(path, name), name)
  }

  override def createTempOutput(prefix: String, suffix: String, context: IOContext): IndexOutput = {
    throw new UnsupportedOperationException("unsupport createTempOutput ")
  }

  override def sync(names: util.Collection[String]): Unit = {}

  override def syncMetaData(): Unit = {}

  override def openInput(name: String, context: IOContext): IndexInput = {
    val p: Path = new Path(path, name)
    logDebug("openHdfsInput "+p)
    new HdfsIndexInput(fileSystem, p, name)
  }


  /**
    * Rename file
    * @param source
    * @param dest
    */
  override def rename(source: String, dest: String): Unit = {
    val sourcePath = new Path(path, source)
    val destPath = new Path(path, dest)
    fileContext.rename(sourcePath, destPath);
  }

  /**
    * File length
    * @param name
    * @return
    */
  override def fileLength(name: String): Long = {
    val fileStatus: FileStatus = fileSystem.getFileStatus(new Path(path, name));
    fileStatus.getLen();
  }


  /**
    * Delete file
    * @param name
    */
  override def deleteFile(name: String): Unit = {
    val file: Path = new Path(path, name)
    logDebug("Deleting file: "+file)
    fileSystem.delete(file, false)
  }

  /**
    * List all files under this directory
    * @return
    */
  override def listAll(): Array[String] = {
    FsUtils.listAll(fileSystem, path).map(name => toNormalName(name))
  }

}

object HdfsDirectory{
  val BLOCK_SIZE: Int = 64*1024 // 64 Kb
  val LF_EXT: String = ".lf"


  def apply(pathStr: String, conf: Configuration): HdfsDirectory = {
    val path =new Path(pathStr)
    new HdfsDirectory(path, HdfsLockFactory(), conf, BLOCK_SIZE )
  }

  def apply(path: Path, conf: Configuration): HdfsDirectory = {
    new HdfsDirectory(path, HdfsLockFactory(), conf, BLOCK_SIZE )
  }

  private def toNormalName(name: String): String = {
    if (name.endsWith(LF_EXT)) {
      return name.substring(0, name.length() - 3)
    }
    return name
  }



}