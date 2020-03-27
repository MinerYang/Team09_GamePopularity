import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object SteamDataCleansing {
  def main(args: Array[String]) {
    //linux files or sth like HDFS
    //edit configurations => program arguments: ./Steam.csv
        if (args.length == 0) {
          System.err.println("Usage: SparkWordCount <inputfile>")
          System.exit(1)
        }

    //run locally, and set up number of threads
    //e.g. setMaster("local[2]")
    val conf = new SparkConf().setAppName("SteamDataCleansing").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //wordcount: counting number of lines containing "spark"
    //    val count = sc.textFile(args(0)).filter(line => line.contains("Spark")).count()
    //    println("count=" + count)

    val ttw = sc.textFile(args(0)).filter(_.contains("Total War")).collect()
    ttw foreach println
    sc.stop()
  }
}
