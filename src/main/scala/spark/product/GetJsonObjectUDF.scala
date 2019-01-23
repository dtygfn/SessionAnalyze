package spark.product

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.api.java.UDF2

/**
  * UDF2<String, String, String>
  * 前两个类型是值传进来的值的类型
  * 第一个类型代表json格式的字符串
  * 第二个代表要获取字段值的字段名称
  * 第三个类型代表返回json串里的某个字段的值
  */
class GetJsonObjectUDF extends UDF2[String,String,String] {
  override def call(json: String, field: String): String = {
    try {
      val jsonObject = JSON.parseObject(json)
      return jsonObject.getString(field)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    null
  }
}
