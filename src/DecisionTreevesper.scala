import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.{SparkConf, SparkContext}

object DecisionTreevesper {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DedecisionTree").setMaster("local[3]")
      .setJars(Seq(""))
    val sc = new SparkContext(sparkConf)
    val rawData = sc.textFile("hdfs://192.168.19.139:9000/user/vesper/data/covtype.data", 4)
    val data = rawData.map { line =>
      val values = line.split(",").map(_.toDouble)
      val featureVector = Vectors.dense(values.init)
      val label = values.last - 1
      LabeledPoint(label, featureVector)
    }
    val model = DecisionTree.trainClassifier(data, 7, Map[Int, Int](), "entropy", 5, 10)
    println(model.toDebugString)
  }
}
