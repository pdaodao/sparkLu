package top.myetl.lucenerdd.store

import java.io.OutputStream

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.lucene.store.OutputStreamIndexOutput
import top.myetl.lucenerdd.util.FsUtils

/**
  * Created by pengda on 17/1/4.
  */
class HdfsIndexOut(val resourceDescription: String,val name: String,
                   val out: OutputStream, val bufferSize: Int)
  extends OutputStreamIndexOutput(resourceDescription, name, out, bufferSize){

  def this(fileSystem: FileSystem, path: Path, name: String, bufferSize: Int) = {
     this("HdfsIndexOut "+name, name, FsUtils.getOutputStream(fileSystem, path), bufferSize)
  }

}

object HdfsIndexOut{
  val BUFFER_SIZE: Int = 16384

  def apply(fileSystem: FileSystem, path: Path, name: String): HdfsIndexOut = {
    new HdfsIndexOut(fileSystem, path, name, BUFFER_SIZE)
  }
}
