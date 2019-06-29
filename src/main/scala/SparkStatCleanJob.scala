

import MyUtil.AccessConvertUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 使用Spark完成我们的数据清洗操作
  */
object SparkStatCleanJob {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("SparkStatCleanJob")
      .config("spark.sql.parquet.compression.codec","gzip")
      .master("local[*]").getOrCreate()

    val accessRDD = spark.sparkContext.textFile("F:\\BigData\\access\\part-00000")

    //accessRDD.take(10).foreach(println)

    //RDD ==> DF
    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)),
      AccessConvertUtil.struct)

    //    accessDF.printSchema()
    //    accessDF.show(false)

    accessDF
      .coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
      .partitionBy("day")
   .save("F:\\BigData\\clean2")


    spark.stop
  }
}
