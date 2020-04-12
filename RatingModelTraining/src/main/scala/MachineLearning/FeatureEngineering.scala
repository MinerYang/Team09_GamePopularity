package MachineLearning

import MachineLearning.PipelineTransfomer.{fs, indexer, t0, t1, t10, t11, t12, t2, t3, t4, t5, t6, t7, t8, t9}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}

object FeatureEngineering {
  lazy val appName = "FeatureEngineering"
  lazy val master = "local[*]"
  lazy val threshold = 0.05
  val path = "/Users/mineryang/Desktop/Team09_GamePopularity/RatingModelTraining"

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder.master(master).appName(appName).getOrCreate()
    ss.sparkContext.setLogLevel("WARN")

    // import cleaned and preprocessed dataset for feature engineering
    val origindf: DataFrame = ss.read.parquet(s"$path/cleandata.parquet")
    origindf.show(5)
    origindf.printSchema()

    //construct pipeline1 for feature selection
    val stages1 = Array(indexer,t0, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12)
    val pipeline1 = new Pipeline()
      .setStages(stages1)
    val df1 = pipeline1.fit(origindf).transform(origindf)
    print("Completed : 30 %\n")

    //pipeline2 for feature assembler
    val assemb: VectorAssembler = fs
    val final_data = assemb.transform(df1)
    final_data.show(5)
    print("Completed : 80 %\n")

    //save featued data
    final_data.write.parquet(s"$path/featuredData1.parquet")
    println("save featured data to loacal featuredData.parquet")
    print("Completed : 100 %\n")
  }
}
