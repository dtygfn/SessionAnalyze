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
import java.{lang, util}

import domain.AreaTop3Product

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

    // 生成各区域商品点击次数
    // 字段：area,product_id,click_count,city_info
    generateTempAreaProductClickCountTable(spark)

    // 生成包含完整商品信息的各区域各商品点击次数的临时表
    // 技术点4：内置if函数的使用
    generateTempAreaFullProductClickCountTable(spark)


    // 使用开窗函数获取各个区域点击次数top3热门商品
    // 技术点5：开窗函数
    val areaTop3ProductRDD = getAreaTop3ProductRDD(spark)

    // 就这个业务需求而言，最终的结果是很少
    // 一共就几个区域，每个区域只取top3的商品，最终的数据也就几十个
    // 所以可以直接将数据collect到Driver端，再用批量插入的方式一次性插入数据库表
    val rows = areaTop3ProductRDD.collect

    // 存储
    persistAreaTop3Product(taskId, rows)
    sc.stop()


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
    "select " +
      "city_id, " +
      "click_product_id product_id " +
      "from user_visit_action " +
      "where click_product_id is not null " +
      "and date>='" + startDate + "'" +
      "and date<='" + endDate + "'"


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
      val cityInfo = tup._2._2
      val productId = clickAction.getLong(1)
      val cityName = cityInfo.getString(1)
      val area = cityInfo.getString(2)

      Row(cityId,cityName,area,productId)
    })

    // 构建schema信息
    val structFields = new util.ArrayList[StructField]
    structFields.add(DataTypes.createStructField("city_id",DataTypes.LongType,true))
    structFields.add(DataTypes.createStructField("city_name",DataTypes.StringType,true))
    structFields.add(DataTypes.createStructField("area", DataTypes.StringType, true))
    structFields.add(DataTypes.createStructField("product_id", DataTypes.LongType, true))

    val schema = DataTypes.createStructType(structFields)

    // 生成DataFreame
    val df = spark.createDataFrame(mappedRDD,schema)

    // 注册为临时表,字段：cityId, cityName, area, productId
    df.createTempView("tmp_click_product_basic")

  }

  /**
    * 生成各区域商品点击次数
    * @param spark
    */
  def generateTempAreaProductClickCountTable(spark: SparkSession): Unit = {
    // 计算出各区域商品点击次数
    // 可以获取到每个area下每个product_id的城市信息，并拼接字符串
    val sql =
    "select " +
      "area," +
      "product_id," +
      "count(*) click_count," +
      "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos " +
      "from tmp_click_product_basic " +
      "group by area,product_id"
    val df = spark.sql(sql)

    // area,product_id,click_count,city_info
    df.createTempView("tmp_area_product_click_count")
  }

  /**
    * 生成包含完整商品信息的各区域各商品点击次数的临时表
    * @param spark
    */
  def generateTempAreaFullProductClickCountTable(spark: SparkSession): Unit = {
    /**
      * 将之前得到的各区域商品点击次数表(tmp_area_product_click_count)的product_id字段
      * 去关联商品信息表(product_info)的product_id
      * 其中product_status需要特殊处理：0,1分别代表了自营和第三方商品，放在了一个json里
      * 实现GetJsonObjectUDF()函数是从json串中获取指定字段的值
      * if()函数进行判断，如果product_status为0，就
      是自营商品，如果为1，就是第三方商品
      * 此时该表的字段有：
      * area,product_id,click_count,city_infos,product_name,product_status
      */
    val sql =
      "select " +
        "tapcc.area," +
        "tapcc.product_id," +
        "tapcc.click_count," +
        "tapcc.city_infos," +
        "pi.product_name," +
        "if(get_json_object(pi.extend_info,'product_status')='0'," +
        "'Self','Third Party') product_status " +
        "from tmp_area_product_click_count tapcc " +
        "join product_info pi " +
        "on tapcc.product_id=pi.product_id"

    val df = spark.sql(sql)

    df.createTempView("tmp_area_fullprod_click_count")
  }

  /**
    * 获得区域top3区域热门商品
    * @param spark
    */
  def getAreaTop3ProductRDD(spark: SparkSession) = {
    /**
      * 使用开窗函数进行子查询
      * 按照area进行分组，给每个分组内的数据按照点击次数进行降序排序，并打一个行标
      * 然后在外层查询中，过滤出各个组内行标排名前3的数据
      * 按照区域进行分级：
      * 华北、华东、华南、华中、西北、西南、东北
      * A级：华北、华东
      * B级：华南、华中
      * C级：西北、西南
      * D级：东北
      */
    val sql =
      "select " +
        "area," +
        "case " +
        "when area='华北' or area='华东' then 'A级' " +
        "when area='华南' or area='华中' then 'B级' " +
        "when area='西北' or area='西南' then 'C级' " +
        "else 'D级' " +
        "end area_level," +
        "product_id," +
        "click_count," +
        "city_infos," +
        "product_name," +
        "product_status " +
        "from(" +
        "select " +
        "area," +
        "product_id," +
        "click_count," +
        "city_infos," +
        "product_name," +
        "product_status," +
        "ROW_NUMBER() OVER (PARTITION BY area ORDER BY click_count DESC) rank " +
        "from tmp_area_fullprod_click_count " +
        ") t " +
        "where rank <= 3"
    val df = spark.sql(sql)

    df.rdd
  }

  /**
    * 存储结果数据
    *
    * @param taskId
    * @param rows
    */
  def persistAreaTop3Product(taskId: lang.Long, rows: Array[Row]) ={
    val list = new  util.ArrayList[AreaTop3Product]
    for (row <- rows) {
      val areaTop3Product = new AreaTop3Product
      areaTop3Product.setTaskid(taskId)
      areaTop3Product.setArea(row.getString(0))
      areaTop3Product.setAreaLevel(row.getString(1))
      areaTop3Product.setProductid(row.getLong(2))
      areaTop3Product.setClickCount(row.getLong(3))
      areaTop3Product.setCityInfos(row.getString(4))
      areaTop3Product.setProductName(row.getString(5))
      areaTop3Product.setProductStatus(row.getString(6))

      list.add(areaTop3Product)
    }

    val areaTop3ProductDAO = DAOFactory.getAreaTop3ProductDAO
    areaTop3ProductDAO.insertBatch(list)

  }


}
