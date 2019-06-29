import org.apache.spark.{SparkConf, SparkContext}


object dataCleaning {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("datacleaning")
    val sc = new SparkContext(sparkconf)
    sc.textFile("D:\\学习资料\\视频\\大数据\\Spark\\以慕课网日志分析为例 进入大数据 Spark SQL 的世界\\access.20161111.log")
      .map(tp => {
        val v = tp.split(" ")
        //日期时间格式 [dd/MMM/yyyy:HH:mm:ss Z]
        val time = v(3) + " " + v(4)
        val ip = v(0)
        val url = v(11).replace("\"", "")
        //流量
        val traffic = v(9)


        if (!url.equals("-") && url.indexOf("http://www.imooc.com/") == 0) {
          val splits = url.substring(url.indexOf("http://www.imooc.com/") + "http://www.imooc.com/".length).split("/")
          if (traffic.forall(i => Character.isDigit(i))){
            MyUtil.DateUtils.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip
          }

        }
      })
      .filter(_ != ()).coalesce(1)
      .saveAsTextFile("F:\\BigData\\access")
    sc.stop
  }

}
