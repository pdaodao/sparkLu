package top.myetl.lucenerdd.rdd

import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.conf.Configuration
import org.apache.lucene.document.Document
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.search.IndexSearcher
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import top.myetl.lucenerdd.convert.DocToBean
import top.myetl.lucenerdd.query.MyQuery
import top.myetl.lucenerdd.query.MyQuery.{matchAll, must}

import scala.reflect.ClassTag



class LuceneQueryRDD[T:ClassTag](
                             sc: SparkContext,
                             val prev: LuceneSearcherRDD,
                             depts: Seq[Dependency[_]] = Nil)
                           (docConversion: DocToBean[T]) extends RDD[T](sc, depts){

  def this(prev: LuceneSearcherRDD)(docConversion: DocToBean[T]) = {
    this(prev.context, prev, Seq(new OneToOneDependency(prev)))(docConversion)
  }


  var condition: MyQuery = _

  def query(q: MyQuery): LuceneQueryRDD[T] = {
    val que = must(q, null)
    condition = que
    this
  }


  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val id = split.index

    val iter: Iterator[IndexSearcher] = firstParent[IndexSearcher].iterator(split, context)

    val searcher = iter.next()

    val q: MyQuery  = if(null == condition) matchAll else condition

    val topDocs = searcher.search(q.build(), 10)

    val total = topDocs.totalHits
    val scoreDocs = topDocs.scoreDocs
    val docsIter = scoreDocs.iterator

    new Iterator[T]{
      override def hasNext: Boolean = docsIter.hasNext

      override def next(): T = {
        val score = docsIter.next()
        lazy val document = searcher.doc(score.doc)
        docConversion.toBean(score, document)
      }
    }

  }


  override protected def getPartitions: Array[Partition] = firstParent[T].partitions




  override def checkpoint() {
    // Do nothing. Lucene RDD should not be checkpointed.
  }

}


