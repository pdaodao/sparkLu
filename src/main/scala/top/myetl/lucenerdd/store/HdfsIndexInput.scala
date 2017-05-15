package top.myetl.lucenerdd.store

import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.lucene.store.{BufferedIndexInput, IndexInput}
import org.apache.spark.Logging

/**
  * Created by pengda on 17/1/4.
  */
class HdfsIndexInput(fileSystem: FileSystem,path: Path, resourceDescription: String)
  extends BufferedIndexInput(resourceDescription) with Logging{

  logInfo("hdfsIndexInput: "+path)

  val fileLength: Long = fileSystem.getFileStatus(path).getLen
  val inputStream: FSDataInputStream = fileSystem.open(path)
  var isClone: Boolean = false

  override def seekInternal(pos: Long): Unit = {
  }

  override def readInternal(b: Array[Byte], offset: Int, length: Int): Unit = {
    inputStream.readFully(getFilePointer, b, offset, length)
  }

  override def length(): Long = fileLength

  override def close(): Unit = {
    if(!isClone){
      logDebug("close HdfsIndexInput .."+path)
      inputStream.close()
    }
  }

  override def clone(): BufferedIndexInput = {
    logDebug("clone HdfsIndexInput "+path)
    val in: HdfsIndexInput = super.clone().asInstanceOf[HdfsIndexInput]
    in.isClone = true
    in
  }

}
