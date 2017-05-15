package top.myetl.lucenerdd.rdd


import top.myetl.lucenerdd.convert.LuceneDocConvert._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.lucene.document.Document
import org.apache.lucene.index.{DirectoryReader, IndexReader}
import org.apache.lucene.search.IndexSearcher
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import top.myetl.lucenerdd.convert.DocToBean
import top.myetl.lucenerdd.store.HdfsDirectory
import top.myetl.lucenerdd.util.FsUtils
import top.myetl.lucenerdd.query.MyQuery
import top.myetl.lucenerdd.query.MyQuery._
import scala.reflect.ClassTag

private[lucenerdd] class LuceneRDDPartition[T :ClassTag](
  idx: Int,
  private val path: String) extends Partition{

  private def getReader(): IndexReader = {
    val configuration = new Configuration()
    println(index+"-------  open directory ... "+path+" --- "+this)
    val directory = HdfsDirectory(new Path(this.path), configuration)
    val reader = DirectoryReader.open(directory)
    reader
  }
  private lazy val reader = getReader()

  def getSearcher(): IndexSearcher = synchronized{
    new IndexSearcher(reader)
  }

  override def index: Int = idx
}

class LuceneRDD[T:ClassTag](
                              sc: SparkContext,
                              val tableName: String,
                              val prev: RDD[_] = null,
                              depts: Seq[Dependency[_]] = Nil,
                              private val query: MyQuery = null)
                              (docConversion: DocToBean[T]) extends RDD[T](sc, depts){

  def this(prev: LuceneRDD[_], query: MyQuery)(docConversion: DocToBean[T]) = {
    this(prev.context, prev.tableName, prev, Seq(new OneToOneDependency(prev)), query)(docConversion)
  }

  clearDependencies()

  private var topK: Int = Int.MaxValue

  def query(q: MyQuery): LuceneRDD[T] = {
    val que = must(q, query)
    new LuceneRDD[T](this, que)(docConversion)
  }

  def fields(): Seq[String] = {
    new LuceneRDD[Seq[String]](this, null)(GetFields).take(1).head
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {

    val p = split.asInstanceOf[IndexRDDPartition[_]]
    val start = System.nanoTime()
    val searcher = p.getSearcher()
    val q: MyQuery  = if(null == query) matchAll else query
    val topDocs = searcher.search(q.build(), this.topK)
    val t2 = System.nanoTime()

    println(q.toString+"-> time(s)"+(System.nanoTime - start) / 1e9)

    val total = topDocs.totalHits
    val scoreDocs = topDocs.scoreDocs
    val iter = scoreDocs.iterator

    new Iterator[T]{
      override def hasNext: Boolean = iter.hasNext

      override def next(): T = {
        val score = iter.next()
        lazy val document = searcher.doc(score.doc)
        docConversion.toBean(score, document)
      }
    }
  }

  private def buildPartitions: Array[Partition] = {
    val baseDir = FsUtils.getHdfsBaseDir(sparkContext.getConf)
    val tableDir = FsUtils.dirName(baseDir, tableName)
    val configuration = SparkHadoopUtil.get.conf
    val fs: FileSystem = FileSystem.get(configuration)
    val paths = FsUtils.listLuceneDir(fs, new Path(tableDir))
    paths.indices.map(i =>
      new IndexRDDPartition[T](i, FsUtils.dirName(tableDir, paths(i)))
    ).toArray
  }

  override protected def getPartitions: Array[Partition] = {
    prev match {
      case t: LuceneRDD[T] => t.partitions
      case _ => buildPartitions
    }
  }

  override def take(num: Int): Array[T] = {
    this.topK = num
    super.take(num)
  }

  override def checkpoint() {
    // Do nothing. Lucene RDD should not be checkpointed.
  }

  override def localCheckpoint(): LuceneRDD.this.type = this

  override def persist(): LuceneRDD.this.type = this

  override def persist(newLevel: StorageLevel): LuceneRDD.this.type = this

}


object LuceneRDD extends Serializable{

  /**
    * SparkContext function
    * Create LuceneRDD by Hdfs file
    * @param sc
    * @param tableName file name table name
    * @return   RDD
    */
  def apply[T: ClassTag](sc: SparkContext, tableName: String)
                        (docConversion: DocToBean[T] ): LuceneRDD[T] = {
    new LuceneRDD[T](sc, tableName)(docConversion)
  }

  /** RDD function
    * Save the RDD save Lucene
    * @param rdd
    * @param tableName
    * @tparam T
    * @return
    */
  def saveToLucene[T : ClassTag](rdd: RDD[T], tableName: String)(docConversion: T => Document): LuceneRDD[T] = {
    null
  }

}
