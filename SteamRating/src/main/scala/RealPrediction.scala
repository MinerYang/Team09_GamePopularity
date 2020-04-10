
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, DoubleType, StructField, StructType}
//import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.util._

object RealPrediction {
  lazy val appName = "realTimePrediction"
  lazy val master = "local[*]"
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder.master(master).appName(appName).getOrCreate()
    ss.sparkContext.setLogLevel("WARN")

    // Load data
//    val loadmodel = ss.read.load("/Users/mineryang/Desktop/PipelineIrisModel2")
    val loadmodel = PipelineModel.load("/Users/mineryang/Desktop/PipelineIrisModel4.0")
    loadmodel.stages

    /**
     * use model to do real-time prediction
     */
//    val schema = new StructType(Array(
//      StructField("SepalLengthCm",DataTypes.DoubleType),
//      StructField("SepalWidthCm",DataTypes.DoubleType),
//      StructField("PetalLengthCm",DataTypes.DoubleType),
//      StructField("PetalWidthCm",DataTypes.DoubleType),
//    ))

//    val user_input = (5.2,2.7,3.9,1.4)
    val user_input = ss.sparkContext.parallelize(Seq(("5.2, 2.7, 3.9, 1.4"))) // mock for real data
    val schema = new StructType()
      .add("SepalLengthCm", DoubleType, true)
      .add("SepalWidthCm", DoubleType, true)
      .add("PetalLengthCm", DoubleType, true)
      .add("PetalWidthCm", DoubleType, true)

    val testdata = user_input.map(line => {
      val Array(a,b,c,d) = line.split(",")
      Row(
        a.toDouble,
        b.toDouble,
        c.toDouble,
        d.toDouble
      )
    })
    val testdf = ss.sqlContext.createDataFrame(testdata, schema)
    testdf.show()

    //transform a inputdataframe into a dataframe with features columns
    val prediction = loadmodel.transform(testdf)
    //[SepalLengthCm: double, SepalWidthCm: double ... 7 more fields]
    prediction.show()

//    def typtest(str:String) ={
//      println("ok")
//    }


    // return RDD rows collection
    val prediction_label = prediction.select(prediction.col("predictedLabel")).collect()
    // [Lorg.apache.spark.sql.Row;

    //row: [Iris-versicolor]
    for(row <- prediction_label) {
      val res = row.get(0)
      println(res)
    }


  }

}
