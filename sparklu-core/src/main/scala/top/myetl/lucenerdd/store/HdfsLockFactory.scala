package top.myetl.lucenerdd.store

import java.io.IOException
import java.nio.file.FileAlreadyExistsException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.ipc.RemoteException
import org.apache.lucene.store._
import org.apache.spark.Logging
import top.myetl.lucenerdd.util.FsUtils

/**
  * Created by pengda on 17/1/10.
  */
class HdfsLockFactory extends LockFactory with Logging{

  override def obtainLock(dir: Directory, lockName: String): Lock = {
    if( !(dir.isInstanceOf[HdfsDirectory])){
      throw new UnsupportedOperationException("HdfsLockFactory can only be used with HdfsDirectory subclasses, got dir"+dir)
    }
    val hdfsDir = dir.asInstanceOf[HdfsDirectory]
    val config = hdfsDir.config
    val lockPath = hdfsDir.path
    val lockFile = new Path(lockPath, lockName)
    val lockDir = lockPath.toUri.getPath
    val fs: FileSystem = FileSystem.get(lockPath.toUri, config)
    var file: FSDataOutputStream = null
    var loop = true
    while (loop){
      try{
        if(!fs.exists(lockPath)){
          val success =fs.mkdirs(lockPath)
          if(!success) throw new RuntimeException("Could not create directory:"+lockPath)
        }else{
          // just to check for safe mode
          fs.mkdirs(lockPath)
        }
        file = fs.create(lockFile, false)
        loop = false
      }catch {
        case e: FileAlreadyExistsException => throw new LockObtainFailedException("Cannot obtain lock file:"+lockFile, e)
        case e: RemoteException => {
          if(e.getClassName.equals("org.apache.hadoop.hdfs.server.namenode.SafeModeException")){
            logWarning("The NameNode is in SafeMode - wait 5 seconds and try again")
            try{
              Thread.sleep(5000)
            }catch {
              case e2: InterruptedException => Thread.interrupted()
            }
          }else{
            throw new LockObtainFailedException("Cannot obtain lock file: " + lockFile, e);
          }
        }
        case e: IOException => throw new LockObtainFailedException("Cannot obtain lock file:"+lockFile, e)
      }finally {
        FsUtils.close(lockDir, fs)
      }
    }
    new HdfsLock(config, lockFile)
  }


}

class HdfsLock(val conf: Configuration, val lockFile: Path) extends Lock{
  @volatile var closed: Boolean = false

  private val lock = new Object()

  override def ensureValid(): Unit = {}

  override def close(): Unit = {
    if(!closed){
      closed = true
      val dir = lockFile.toUri.getPath
      def fs: FileSystem = FsUtils.get(lockFile, conf)
      try{

        if(fs.exists(lockFile)){
          try{
            fs.delete(lockFile, false)
          }catch {
            case ed: Exception => println("ed exception "+ed.getMessage)
          }

          if(fs.exists(lockFile)){
            throw new LockReleaseFailedException("failed to delete: " + lockFile)
          }

        }
      }catch {
        case e: Exception => println(" e exception "+e.getMessage)
      }finally {
        FsUtils.close(dir, fs)
      }
    }
  }

  override def toString: String = {
    "HdfsLock(lockFile="+lockFile+")"
  }
}

object HdfsLockFactory{

  def apply(): HdfsLockFactory = new HdfsLockFactory()

}
