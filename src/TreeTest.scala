import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext, sql}

/**
  * 字段信息（每行5个）
  * instant,dteday,season,yr（0,1）,mnth,
  * holiday（0,1）,weekday,workingday,weathersit,temp,
  * atemp,hum,windspeed,casual,registered,
  * cnt
  * 1,2011-01-01,1,0,1,
  * 0,6,0,2,0.344167,
  * 0.363625,0.805833,0.160446,331,654,
  * 985
  *
  * 练得太少了
  * @！！！！@@@@@@@！@@@！！！！！
  */
object TreeTest {
  def main(args: Array[String]): Unit = {

    //    val sparkConf = new SparkConf().setAppName("MGtree").setMaster("local[4]")
    //      .setJars(Seq(""))
    //    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder().master("local[2]").appName("MGTJ").getOrCreate()
    val dataDF = spark.read.option("header", true).format("csv").load("hdfs://192.168.19.139:9000//data/day.csv")
    val rawData = dataDF.rdd
    val data = rawData.collect().map { line =>
      val values = line.mkString(",").split(",")//.map(_.toDouble)
//      val wilderness = values.slice(2,15).indexOf(1,0).toDouble
//      line(1)
      println(line)
    }

  }
}
