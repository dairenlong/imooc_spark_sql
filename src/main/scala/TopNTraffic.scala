import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object TopNTraffic {

  /**
    * 按流量统计最受欢迎的topn课程
    *
    * @param args
    */

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("TopNTraffic").master("local[*]").getOrCreate()
    val df = spark.read.format("parquet")
      .load("F:\\BigData\\clean2")
    /*  import  spark.implicits._
      val topnTraffic = df.filter($"day" === "20161110" && $"cmsType" === "video")
        .groupBy("day", "cmsId")
        .agg(sum("traffic").as("traffics"))

      topnTraffic.select(
        topnTraffic("day"),
        topnTraffic("cmsId"),
        topnTraffic("traffics"),
        row_number().over(Window.orderBy(topnTraffic("traffics").desc)).as("traffucs_row")
      ).filter($"traffucs_row"<=3)
        .show(false)
  */

    df.createOrReplaceTempView("traffic")
    val traffictop3 = spark.sql(
      """
      select day,cmsId,sum(traffic) as traffics from
      traffic
      where day="20161110" and cmsType="video"
      group by day,cmsId
      order by traffics desc
      limit 3

     """.stripMargin)

    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "1234")
    traffictop3.write.jdbc("jdbc:mysql://localhost/test", "toptraffic", properties)

  }


}
