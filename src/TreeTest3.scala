
  import breeze.numerics.sqrt
  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.mllib.linalg.Vectors
  import org.apache.spark.mllib.regression.LabeledPoint
  import org.apache.spark.mllib.linalg.Vector
  import org.apache.spark.rdd.RDD
  import spire.math.Algebraic.Expr.Pow

  object TreeTest3 {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("LineRegression").setMaster("local[2]")
      val sc = new SparkContext(conf)

      val file = sc.textFile("hdfs://master:9000/data/datatext.data")
      val Data = file.map{Lines=>
        val line = Lines.split(",")
        val lab = {
          if(line.last.equals("Iris-setosa")){
            0.0
          }else if(line.last.equals("Iris-versicolor")){
            1.0
          }else{
            2.0
          }
        }
        val arr = line.init.map(_.toDouble)
        val vector = Vectors.dense(arr)
        LabeledPoint(lab,vector)
      }

      val arr = Vectors.dense(Array(4.8,3.0,1.4,0.3))
      val model = Data.map(x=>(distince(x.features,arr).toDouble,x.label)).sortByKey(true).take(5).toMap
      val amodel = model.map(_._2).toArray

      val result = sc.parallelize(amodel).map((_,1)).reduceByKey(_+_).map(x=> (x._2,x._1)).sortByKey(false).map(_._2).take(1)
      for(a<-result){
        println(a)
      }
    }
    def distince(a:Vector,b:Vector): Double ={
      val aa = a.toArray
      val bb = b.toArray
      val number = sqrt(aa.zip(bb).toMap.map(x=>(x._1-x._2)).map(x=>x*x).sum)
      number
    }
    RDD
  }

