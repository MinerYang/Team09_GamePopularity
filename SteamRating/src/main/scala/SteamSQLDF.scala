import org.apache.spark.sql.{DataFrame, SparkSession}

case object SteamSQLDF {
  var appName = "SteamDataCleansing"
  var master = "local[*]"

  def main(args: Array[String]) {
    val ss = SparkSession.builder.master(master).appName(appName).getOrCreate()

    val df: DataFrame = ss.read.format("com.databricks.spark.csv")
      .option("header", "true") //在csv第一行有属性"true"，没有就是"false"
      .option("inferSchema", true.toString) //这是自动推断属性列的数据类型
      .load("Steam.csv")

    df.show()
  }
}
