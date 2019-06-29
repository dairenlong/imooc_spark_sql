import java.util.Properties

import breeze.linalg.sum
import org.apache.spark.sql.SparkSession

/**
  * 最受欢迎的TopN课程
  */
object TopNStatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("TopNStatJob")
      //是否自动推断类型 默认true 自动推断 ，这里设置为不自动断
      .config("spark.sql.sources.partitionColumnTypeInference.enabled",false)
      .getOrCreate()
   // import org.apache.spark.sql.functions._
    import spark.implicits._

    val df = spark.read.format("parquet")
      .load("F:\\BigData\\clean2")
/*    df
      .filter($"day"==="20161110"&&$"cmsType"==="video")
      .groupBy("day","cmsId")

      .sum("traffic")
      .orderBy($"sum(traffic)".desc)
      .show(10,false)*/

    df.createOrReplaceTempView("StatJob")
    val statJob= spark.sql(
      """
        |select day,cmsId,sum(traffic) from
        |StatJob
        |where day="20161110" and cmsType="video"
        |group by day ,cmsId
        |order by sum(traffic) desc
        |limit 10
      """.stripMargin)

    //写入mysql
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","1234")
    statJob.write.jdbc("jdbc:mysql://localhost/test","StatJob",properties)

    /*df
        .printSchema()
      df
      .show(false)
*/
    spark.stop()
  }
}
