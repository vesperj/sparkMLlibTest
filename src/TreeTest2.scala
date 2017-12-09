//import breeze.numerics.abs
//
//
//
//import breeze.numerics.pow
//import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.mllib.regression.LabeledPoint
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//
//import scala.collection.mutable.Map
//
//
//object TreeTest2 {
//
//
//    def getGroup(): Array[Array[Double]] = {
//      return  Array(Array(4.8, 3.0, 1.4, 0.3), Array(5.1, 3.8, 1.6, 0.2), Array(4.6, 3.2, 1.4, 0.2), Array(7.0, 3.2, 4.7, 1.4)
//        , Array(6.4, 3.2, 4.5, 1.5), Array(6.9, 3.1, 4.9, 1.5), Array(6.7, 3.0, 5.2, 2.3), Array(6.3, 2.5, 5.0, 1.9), Array(6.5, 3.0, 5.2, 2.0))
//    }
//    def getLabels(): Array[Char] = {
//      return Array('A', 'B', 'C')
//    }
//
////    def classify0(inX: Array[Double], dataSet: Array[Array[Double]], labels: Array[Char], k: Int): Char = {
////
//////      val sortedDisIndicies = dataSet.map { x =>
//////
//////        val v1 = x(0) - inX(0)
//////        val v2 = x(1) - inX(1)
//////        val v3 =x(2)-inX(2)
//////        val v4 =x(3)-inX(3)
//////        v1 * v1 + v2 * v2 + v3 * v3 + v4 * v4
//////      }.zipWithIndex.sortBy(f => f._1).map(f => f._2)
//////      var classsCount: Map[Char, Int] = Map.empty
//////      for (i <- 0 to k - 1) {
//////        val voteIlabel = labels(sortedDisIndicies(i))
//////        classsCount(voteIlabel) = classsCount.getOrElse(voteIlabel, 0) + 1
//////      }
//////      classsCount.toArray.sortBy(f => -f._2).head._1
////
////    }
//
//
//    def main(args: Array[String]): Unit = {
//      val conf = new SparkConf().setAppName("MyKNN").setMaster("local[2]")
//      val sc = new SparkContext(conf)
//      sc.setLogLevel("WARN")
//      val rowData = sc.textFile("hdfs://master:9000//user/wz/Files/iris.dat")
//      val data =rowData.map{line=>
//        val value = line.slice(0,4).split(",").map(_.toDouble)
//        val r =classify0(value, getGroup(), getLabels(), 7)
//        println(r)
//        r
//      }
//      println(data.collect)
//    }
//
//
//}
