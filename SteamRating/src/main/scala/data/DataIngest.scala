package data
import org.apache.spark.ml.feature.{ChiSqSelector, VectorAssembler}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case object DataIngest{

  /**
   * transfer a column type from String to Array by pattern
   * @param df DataFrame
   * @param colname column name
   * @return a new DataFrame with column transferred
   */
  def colParse(df: DataFrame, colname: String): DataFrame = {
    df.withColumn(colname, split(df(colname),";"))
  }

  /**
   * Merge columns without label into a vector column named "features"
   * @param df DataFrame
   * @return DataFrame with vector-feature column
   */
  def MergeToVec(df: DataFrame, label:String):DataFrame = {
    var colArr = df.columns
    colArr = colArr.filter(! _.contains(label))
    val assembler = new VectorAssembler()
      .setInputCols(colArr)
      .setOutputCol("features")
    val output = assembler.transform(df)
    output
  }

  /**
   * convert Array to dense Vector
   * @return
   */
  def ArrayToVec = udf((array : Seq[Integer]) => {
    Vectors.dense(array.toArray.map(_.toDouble))
  })


  def chiSqSelect(df: DataFrame, NumTopFeatures: Int, featurCol: String, labelCol: String): Unit = {
    val selector = new ChiSqSelector()
      .setNumTopFeatures(NumTopFeatures)
      .setFeaturesCol(featurCol)
      .setLabelCol(labelCol)
      .setOutputCol("selectedFeatures")
    val result = selector.fit(df)
    println(s"ChiSqSelector output with top ${selector.getNumTopFeatures} features selected")

    //show
    val col = df.columns
    var map : Map[Int, String] = Map()
    for(i <- 0 until col.length-1){
      //println(i + ":" + col(i))
      map += (i -> col(i))
    }
    for(k <- result.selectedFeatures){
      println(k+ ": "+ map.get(k).getOrElse("default"))
    }
      result.transform(df)
  }






}
