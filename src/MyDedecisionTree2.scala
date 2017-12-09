import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.{SparkConf, SparkContext}

object MyDedecisionTree2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("decisionTreeAll").setMaster("local[4]" /*"spark://192.168.19.139:7077"*/)
      .setJars(Seq(""))
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val rawData = sc.textFile("hdfs://192.168.19.139:9000/user/vesper/data/covtype.data", 4)
    val data = rawData.map { line =>
      val values = line.split(",").map(_.toDouble)
      val featureVector = Vectors.dense(values.init)
      val label = values.last - 1
      LabeledPoint(label, featureVector)
    }

    val Array(trainData, cvData, testData) = data.randomSplit(Array(0.6, 0.2, 0.2))
    trainData.cache()
    cvData.cache()
    testData.cache()
    val evaluations =
      for (impurity <- Array("gini", "entropy");
           depth <- Array(5,10,15, 20, 25, 30);
           bins <- Array(10, 50, 100, 150))
        yield {
          val model = DecisionTree.trainClassifier(trainData, 7, Map[Int, Int](), impurity, depth, bins)
          val predictionsAndLabels = cvData.map(example => (model.predict(example.features), example.label))
          val accuracy = new MulticlassMetrics(predictionsAndLabels).accuracy
          ((impurity, depth, bins), accuracy)
        }
    evaluations.sortBy(_._2).reverse.foreach(println)
  }
}
