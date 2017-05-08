package top.myetl.lucenerdd.rdd

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.lucene.index.{DirectoryReader, IndexReader}
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import top.myetl.lucenerdd.store.HdfsDirectory
import top.myetl.lucenerdd.util.FsUtils

import org.apache.lucene.search.IndexSearcher


class LuceneSearcherRDDPartition(   idx: Int,
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

class LuceneSearcherRDD( sc: SparkContext,
                             val tableName: String,
                             depts: Seq[Dependency[_]] = Nil)  extends RDD[IndexSearcher](sc, depts){

  clearDependencies()

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[IndexSearcher] = {

    val p = split.asInstanceOf[LuceneSearcherRDDPartition]
    val start = System.nanoTime()
    val searcher = p.getSearcher()
    val t2 = System.nanoTime()
    val isHasNext = new AtomicBoolean(true)


    new Iterator[IndexSearcher]{

      override def hasNext: Boolean = {
        val is = isHasNext.get()
        isHasNext.set(false)
        is
      }
      override def next(): IndexSearcher = searcher
    }

  }

  private def buildPartitions: Array[Partition] = {

    val baseDir = FsUtils.getHdfsBaseDir(sparkContext.getConf)
    val tableDir = FsUtils.dirName(baseDir, tableName)
    val configuration = SparkHadoopUtil.get.conf
    val fs: FileSystem = FileSystem.get(configuration)
    val paths = FsUtils.listLuceneDir(fs, new Path(tableDir))

    paths.indices.map(i =>
      new LuceneSearcherRDDPartition(i, FsUtils.dirName(tableDir, paths(i)))
    ).toArray

  }

  override protected def getPartitions: Array[Partition] = {
    buildPartitions
  }


  override def checkpoint() {
    // Do nothing. Lucene RDD should not be checkpointed.
  }

}