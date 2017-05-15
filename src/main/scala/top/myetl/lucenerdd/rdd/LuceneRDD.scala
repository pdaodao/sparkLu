package top.myetl.lucenerdd.rdd

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.lucene.search.IndexSearcher
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import top.myetl.lucenerdd.convert.DocToBean
import top.myetl.lucenerdd.query.MyQuery
import top.myetl.lucenerdd.query.MyQuery.must
import top.myetl.lucenerdd.util.FsUtils

import scala.reflect.ClassTag

/**
  * 查询数据
  */
class LuceneRDD[T: ClassTag](prev: IndexReadRDD)(docConversion: DocToBean[T])
  extends RDD[T](prev.sparkContext, List(new OneToOneDependency(prev))){

  clearDependencies()

  var condition: MyQuery = _
  var topK: Int = 20

  def query(q: MyQuery): LuceneRDD[T] = {
    condition = q
    this
  }

  override def take(num: Int): Array[T] = {
    topK = num
    super.take(num)
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val id = split.index
    val iter: Iterator[IndexSearcher] = firstParent[IndexSearcher].iterator(split, context)
    val searcher = iter.next()

    val q: MyQuery  = if(null == condition) MyQuery.matchAll else condition

    val topDocs = searcher.search(q.build(), topK)

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

  override protected def getPartitions: Array[Partition] = firstParent.partitions

}

/**
  * 读取索引时使用
  * 这里用来把 Directory IndexReader IndexSearcher 缓存起来
  */
class IndexReadRDD( _sc: SparkContext,
                    tableName: String,
                    deps: Seq[Dependency[_]] = Nil)
  extends RDD[IndexSearcher](_sc, deps){

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[IndexSearcher] = {
    val p =  split.asInstanceOf[IndexRDDPartition]

    new Iterator[IndexSearcher]{
      val isHasNext = new AtomicBoolean(true)
      override def hasNext: Boolean = {
        val is = isHasNext.get()
        isHasNext.set(false)
        is
      }
      override def next(): IndexSearcher = p.indexSearcher
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val baseDir = FsUtils.getHdfsBaseDir(sparkContext.getConf)
    val tableDir = FsUtils.dirName(baseDir, tableName)
    val tablePath = new Path(tableDir)

//    val configuration = SparkHadoopUtil.get.conf
    val configuration = new Configuration()
    val fs: FileSystem = FsUtils.get(tablePath, configuration)
    val paths = FsUtils.listLuceneDir(fs, tablePath)
    FsUtils.close(fs)

    paths.indices.map(i =>
      new IndexRDDPartition(i, FsUtils.dirName(tableDir, paths(i)))
    ).toArray
  }

  override def persist(newLevel: StorageLevel): IndexReadRDD.this.type = {
    super.persist(StorageLevel.MEMORY_ONLY)
  }

}