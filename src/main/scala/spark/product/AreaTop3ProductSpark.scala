package spark.product

import com.alibaba.fastjson.JSON
import conf.ConfigurationManager
import constant.Constants
import dao.factory.DAOFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.{SparkConf, SparkContext}
import spark.util.SparkUtils
import test.MockData
import util.ParamUtils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * 按区域统计top3热门的商品
  */
object AreaTop3ProductSpark {



  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_PRODUCT)
    SparkUtils.setMaster(conf)
    val sc = new SparkContext(conf)
    val spark = SparkUtils.getSparkSession()

    // 生成模拟数据
    MockData.mock(sc,spark)

    // 注册自定义函数
    spark.udf.register("group_concat_distinct", new GroupConcatDistinctUDAF)
    spark.udf.register("concat_long_string", new ConcatLongStringUDF, DataTypes.StringType)
    spark.udf.register("get_json_object", new GetJsonObjectUDF, DataTypes.StringType)

    // 查询任务，获取任务信息
    val taskId = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_PRODUCT)
    val taskDAO = DAOFactory.getTaskDAO
    val task = taskDAO.findById(taskId)

    if (task == null ){
      println("没有获取到对应taskid的信息")
    }

    val taskParam = JSON.parseObject(task.getTaskParam)

    // 获取使用者指定的开始时间和结束时间
    val startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)

    // 查询用户指定日期范围内的点击行为数据<city_id,点击行为>
    // 技术点1：Hive数据源的使用
    //<city_id,click_product_id>
    val cityId2ClickActionRDD = getCityId2ClickActionRDD(spark,startDate,endDate)

    // 从MySQL表（city_info）中查询城市信息，返回的格式为：<cityId, cityInfo>
    // 技术点2：异构数据源MySQL的使用
    val cityId2CityInfoRDD = getCityId2CityInfoRDD(spark)

    // 生成点击商品基出信息表
    // 技术点3：将RDD转为DataFrame，并注册临时表
    // 字段：cityId，cityName，area，productId
    geneateTempClickProductBasicTable(spark,cityId2ClickActionRDD,cityId2CityInfoRDD)
  }


  /**
    * 查询指定日期范围内的点击行为数据
    *
    * @param spark
    * @param startDate
    * @param endDate
    * @return
    */
  def getCityId2ClickActionRDD(spark: SparkSession, startDate: String, endDate: String) = {
    // 第一个限定：click_product_id限定为不为空的访问行为，这个字段的值就代表点击行为
    // 第二个限定：在使用者指定的日期范围内的数据
    val sql =
      "select "+
      "city_id,click_product_id"+
      "from user_visit_action"+
      "where chick_product_id is not null"+
      "and date >='"+startDate+"'"+
      "and date <='"+endDate+"'"

    val clickActionDF = spark.sql(sql)

    // 把生成的DateFreame转换为RDD
    val clickActionRDD = clickActionDF.rdd

    val cityId2ClickActionRDD = clickActionRDD.map(row=>(row.getLong(0),row))
    cityId2ClickActionRDD
  }

  /**
    * 获取城市信息
    *
    * @param spark
    * @return
    */
  def getCityId2CityInfoRDD(spark: SparkSession) = {
    var url = ""
    var user = ""
    var password = ""

    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)

    if (local) {
      url = ConfigurationManager.getProperty(Constants.JDBC_URL)
      user = ConfigurationManager.getProperty(Constants.JDBC_USER)
      password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD)
    } else {
      url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD)
      user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD)
      password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD)
    }

    // 用于存储请求mysql的连接配置信息
    val options = new mutable.HashMap[String,String]()
    options.put("url", url)
    options.put("dbtable", "city_info")
    options.put("user", user)
    options.put("password", password)

    // 获取mysql中city_info表中的数据
    val cityInfoDF = spark.read.format("jdbc").options(options).load

    // 返回RDD
    val cityInfoRDD = cityInfoDF.rdd
    //    val cityId2CityInfoRDD = cityInfoRDD.map(row => (row.getAs[Long](0), row))
    val cityId2CityInfoRDD = cityInfoRDD.map(row => (String.valueOf(row.get(0)).toLong, row))

    cityId2CityInfoRDD

  }


  /**
    * 生成点击商品基础信息临时表
    *
    * @param spark
    * @param cityId2ClickActionRDD
    * @param cityId2CityInfoRDD
    */
  def geneateTempClickProductBasicTable(spark: SparkSession, cityId2ClickActionRDD: RDD[(Long, Row)], cityId2CityInfoRDD: RDD[(Long, Row)]): Unit = {
    val joinedRDD = cityId2CityInfoRDD.join(cityId2ClickActionRDD)

    // 将上面的join后的结果数据转换为一个RDD<Row>
    // 是因为转换后成Row才能将RDD转换为DataFrame
    val mappedRDD = joinedRDD.map(tup=>{
      val cityId = tup._1
      val clickAction = tup._2._1
      val cityInfo = tup._2._1
      val productId = clickAction.getLong(1)
      val cityName  = cityInfo.getString(1)
      val area = cityInfo.getString(2)
      Row(cityId,cityName,area,productId)
    })

    // 构建schema信息
    val structFields = new ListBuffer[StructField]
    structFields += (DataTypes.createStructField("city_id",DataTypes.LongType,true))
    structFields += (DataTypes.createStructField("city_name",DataTypes.StringType,true))

  }



}
