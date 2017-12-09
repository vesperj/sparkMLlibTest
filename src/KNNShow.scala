import breeze.numerics.{abs, pow, sqrt}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object KNNShow {


  def goto1(data: RDD[(Array[Double], String)], arr: (Double, Double, Double, Double)) = {

    val done = data.map{lines =>
      val v1 = lines._1(0)-arr._1
//      println(s"v1 : $v1")
      val v2 = lines._1(1)-arr._2
      val v3 = lines._1(2)-arr._3
      val v4 = lines._1(3)-arr._4
//      println(lines)
      val v5 = sqrt(pow(v1,2)+pow(v2,2)+pow(v3,2)+pow(v4,2))
      println(s"v5:$v5"+lines._2)
      (v5,lines._2)
    }
    println("done:"+done.collect())
    done.sortByKey(true).take(5).toMap
  }

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("MGKNN").setMaster("local[4]")
      .setJars(Seq(""))
    val sc = new SparkContext(sparkConf)
//    sc.setLogLevel("WARN")
    val rawData = sc.textFile("hdfs://192.168.19.139:9000/data/iris.dat")//.filter(f => Character.isDigit(f(0)(0)))
    val data = rawData.map { line =>
      val value = line.split(",")
//      val featureVector = Vectors.dense(value)
//      val label = value.last
//      println(featureVector)
//      LabeledPoint(label,featureVector)

      val a = value.init.map(_.toDouble)
      val b = value.last
      (a,b)
    }
    val arr= (4.8,3.0,1.4,0.3)
    val testData = sc.textFile("hdfs://192.168.19.139:9000/data/test.dat")
    testData.map{lines =>
      val line = lines.split(",").map(x=>(x.toDouble))

      val model = goto1(data, arr)
      val amodel = model.map(_._2).toArray
      val result = sc.parallelize(amodel).map((_, 1)).reduceByKey(_ + _).map((x => (x._2, x._1))).sortByKey(false).map(_._2).take(1)
      for (a <- result) {
        println(a)
      }
    }



//    def goto(data: RDD[LabeledPoint],arr: (Double, Double, Double, Double)):Double={
//      println(data.map(_.features))
//
//
//      println(arr)
//      1.0
//    }

//    println(data.collect())

//    print(data)
//    val data1 = rawData.collect().map(_.split(","))


//    val data = rawData.map(_.split(","))
//    val label = rawData.map(_.split(",").slice(5,5))
    val k = 5



//    data1.foreach(println)


    sc.stop

  }
}
