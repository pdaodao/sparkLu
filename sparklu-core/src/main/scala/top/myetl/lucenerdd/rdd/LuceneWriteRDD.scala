package top.myetl.lucenerdd.rdd


import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.conf.Configuration
import org.apache.lucene.document.Document
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import top.myetl.lucenerdd.convert.BeanToDoc
import top.myetl.lucenerdd.store.HdfsDirectory
import top.myetl.lucenerdd.util.FsUtils

import scala.reflect.ClassTag

/**
  * Created by pengda on 17/1/12.
  */

class LuceneWriteRDD[T: ClassTag] ( val prev: RDD[T],
                                    val tableName: String,
                                    val jobModel: Int = 0  // 0 do nothing 1 write as lucene index 2 merge
                                  )
                                    (docConversion: BeanToDoc[T]) extends RDD[T](prev){

  val baseDir = FsUtils.getHdfsBaseDir(sparkContext.getConf)

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val id = split.index
    val path = FsUtils.dirName(baseDir, tableName, tableName+"-"+id)
    val conf  = new Configuration()
    val directory = HdfsDirectory(path, conf)
    val indexWriter = new IndexWriter(directory,
      new IndexWriterConfig().setOpenMode(OpenMode.CREATE_OR_APPEND))

    println("------------- compute --------------- ")

    val count = new AtomicInteger(0)

    val iter = firstParent[T].iterator(split, context)

    val sp = iter.foreach{ t: T=>
      val doc: Document = docConversion.toDoc(t)
      indexWriter.addDocument(doc)
      if( count.incrementAndGet() >= 1000){
        indexWriter.commit()
        count.set(0)
      }
    }
    indexWriter.commit()
    indexWriter.close()
    directory.close()
//      firstParent[T].iterator(split, context)
    firstParent[T].iterator(split, context).toIterator
  }

  def run(): Long = count()


  override protected def getPartitions: Array[Partition] = firstParent[T].partitions



}
