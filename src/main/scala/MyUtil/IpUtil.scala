package MyUtil

import com.ggstar.util.ip.IpHelper

object IpUtil {

  def parse(ip:String) ={
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]): Unit = {
    print(parse("106.39.41.166"))

  }

}
