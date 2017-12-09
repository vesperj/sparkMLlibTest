import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MyDedecisionTree {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("MGDedecisionTree").setMaster("local[3]") //"spark://192.168.19.139:7077")
      .setJars(Seq(""))
    val sc = new SparkContext(sparkConf)
    Logger.getRootLogger.setLevel(Level.ERROR)
    sc.setLogLevel("ERROR")

    val rawData = sc.textFile("hdfs://192.168.19.139:9000/user/vesper/data/covtype.data", 6)
    val data = rawData.map { line =>
      val values = line.split(",").map(_.toDouble)
      val featureVector = Vectors.dense(values.init)
      val label = values.last - 1
      LabeledPoint(label, featureVector)
    }
    println("data.count" + data.count())
    val Array(trainData, cvData, testData) = data.randomSplit(Array(0.7, 0.15, 0.15))
    trainData.cache()
    cvData.cache()
    testData.cache()

    def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]): MulticlassMetrics = {
      val predictionsAndLabels = data.map(example =>
        (model.predict(example.features), example.label)
      )
      new MulticlassMetrics(predictionsAndLabels)
    }

    val model = DecisionTree.trainClassifier(trainData, 7, Map[Int, Int](), "gini", 7, 500)
    println("Tree model = " + model.toDebugString)
    val metrics = getMetrics(model, cvData)
    println("metrics.precision =" + metrics.precision)
    println("metrics.accuracy = " + metrics.accuracy)
    println("metrics.confusionMatrix : ")
    println(metrics.confusionMatrix)
    (0 until 7).map(
      cat => (metrics.precision(cat), metrics.recall(cat))
    ).foreach(println)

    def classProbabilities(data: RDD[LabeledPoint]): Array[Double] = {
      val countsByCatagory = data.map(_.label).countByValue()
      val counts = countsByCatagory.toArray.sortBy(_._1).map(_._2)
      counts.map(_.toDouble / counts.sum)

    }

    val trainPriorProbabilities = classProbabilities(trainData)
    val cvPriorProbabilities = classProbabilities(cvData)
    val result = trainPriorProbabilities.zip(cvPriorProbabilities).map {
      case (trainProb, cvProb) => trainProb * cvProb
    }.sum
    println("result = " + result)
  }
}
