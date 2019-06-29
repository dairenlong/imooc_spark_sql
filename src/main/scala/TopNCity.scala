
import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
  * 按照地市统计TopN 点击量
  */
object TopNCity {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("TopNCity")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", false)
      .getOrCreate()
    val df = spark.read.load("F:\\BigData\\clean2")
  /*  import spark.implicits._
    val cityTopN = df
      .filter($"day" === "20161110" && $"cmsType" === "video")
      .groupBy("day", "city", "cmsId")
      .agg(count("cmsId").as("cmsIds"))
    val cityTop3 = cityTopN.select(
      cityTopN("day"),
      cityTopN("city"),
      cityTopN("cmsId"),
      cityTopN("cmsIds"),
      row_number().over(Window.partitionBy(cityTopN("city")).orderBy(cityTopN("cmsIds").desc)).as("cmsIds_row")
    ).filter($"cmsIds_row" <= 3)

*/

    df.createOrReplaceTempView("city")
    spark.sql(
      """
        |select tt.day,tt.city,tt.cmsId,tt.cmsIds,tt.city_row from
        |(select t.day,t.city,t.cmsId,t.cmsIds,ROW_NUMBER() OVER(PARTITION BY t.city ORDER BY t.cmsIds desc) as city_row
        |from(
        |select day,city,cmsId,count(1) cmsIds
        |from
        |city
        |where day="20161110" and cmsType= "video"
        |group by day ,city,cmsId
        |) t
        |)tt
        |where city_row<=3

        |
      """.stripMargin).show()



   /* val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","1234")

    cityTop3.write.jdbc("jdbc:mysql://localhost/test","cityTop3",properties)*/

  }
}
