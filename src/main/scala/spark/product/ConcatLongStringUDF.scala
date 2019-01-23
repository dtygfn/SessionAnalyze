package spark.product

import org.apache.spark.sql.api.java.UDF3

/**
  * 将两个字段使用指定的分隔符拼接起来
  * UDF3<Long, String, String, String>中的几个类型分别代表：
  * 前两个类型是指调用者传过来的需要拼接的字段
  * 第三个类型是指用于拼接的分隔符
  * 第四个类型是指返回类型
  */
class ConcatLongStringUDF extends UDF3[Long, String, String, String] {
  override def call(v1: Long, v2: String, split: String): String = {
    String.valueOf(v1)+split+v2
  }
}
