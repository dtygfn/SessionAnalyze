package spark.util

import com.alibaba.fastjson.JSONObject
import conf.ConfigurationManager
import org.apache.spark.{SparkConf, SparkContext}
import constant.Constants
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import test.MockData
import util.ParamUtils

object SparkUtils {
  /**
    * 根据当前是否是本地测试的配置
    * 决定，如何设置SparkConf的master
    */

  def setMaster(conf:SparkConf)={
    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if (local) conf.setMaster("local")
  }


  /**
    * 获取SQLContext
    * 如果spark.local设置为true，那么就创建SQLContext；否则，创建HiveContext
    *
    * @return
    */
  def getSparkSession(): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(Constants.SPARK_APP_NAME_SESSION)
      .getOrCreate()
    spark
  }

  /**
    * 生成模拟数据
    * 如果spark.local配置设置为true，则生成模拟数据；否则不生成
    *
    * @param sc
    * @param spark
    */
  def mockData(sc: SparkContext, spark: SparkSession): Unit = {
    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if (local) MockData.mock(sc, spark)
  }
  /**
    * 获取指定日期范围内的用户行为数据RDD
    *
    * @return
    */
  def getActionRDDByDateRange(spark: SparkSession, taskParam: JSONObject): RDD[Row] = {

    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sqlstr = "select * " +
      "from user_visit_action " +
      "where date>='" + startDate + "' " + "and date <= '" + endDate + "'"

    val actionDF = spark.sql(sqlstr)

    /**
      * 这里就很有可能发生上面说的问题
      * 比如说，Spark SQl默认就给第一个stage设置了20个task，但是根据你的数据量以及算法的复杂度
      * 实际上，你需要1000个task去并行执行
      *
      * 所以说，在这里，就可以对Spark SQL刚刚查询出来的RDD执行repartition重分区操作
      */
    //		return actionDF.javaRDD().repartition(1000);

    actionDF.rdd
  }

}
