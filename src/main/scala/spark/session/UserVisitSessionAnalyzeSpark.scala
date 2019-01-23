package spark.session

import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import constant.Constants
import dao.factory.DAOFactory
import domain._
import org.apache.parquet.it.unimi.dsi.fastutil.ints.{IntArrayList, IntList}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import spark.accumulator.SessionAggrStatAccumulator
import spark.util.SparkUtils
import util._
import java.util

import org.apache.hadoop.mapred.TaskID

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * 获取用户访问Session数据进行分析
  * 1、获取使用者创建的任务信息
  * 任务信息中的过滤条件有：
  * 时间范围：起始日期-结束日期
  * 年龄范围
  * 性别
  * 所在城市
  * 用户搜索的关键字
  * 点击品类
  * 点击商品
  * 2、Spark作业是如何接收使用者创建的任务信息
  * shell脚本通知--SparkSubmit
  * 从MySQL的task表中根据taskId来获取任务信息
  * 3、Spark作业开始数据分析
  */

object UserVisitSessionAnalyzeSpark {



  def main(args: Array[String]): Unit = {
    /**
      * 模板代码
      * 创建配置信息对象
      */

    val conf = new SparkConf()
      .setAppName(Constants.SPARK_APP_NAME_SESSION)
    SparkUtils.setMaster(conf)

    // 上下文对象
    val sc = new SparkContext(conf)

    // 创建sparksql的上下文对象
    val spark = SparkUtils.getSparkSession()
    // 设置检查点
    // sc.checkpointFile("hdfs://node01:9000.....")

    // 生成模拟数据
    SparkUtils.mockData(sc,spark)

    // 创建获取任务信息的实例
    val taskDAO = DAOFactory.getTaskDAO

    // 获取指定任务，获取taskid
    val taskid = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_SESSION)

    val task = taskDAO.findById(taskid)

    if (task == null){
      println(new Date+"亲，你没有获取到taskId对应的task信息")
    }

    // 获取taskId对应的任务信息，也就是task_param字段对应的值
    // task_param的值就使用者提供的查询条件
    val taskParam = JSON.parseObject(task.getTaskParam)

    // 查询指定日期范围内的行为数据（点击、搜索、下单 、支付）
    // 首先要从user_visit_action这张hive表中查询出按照指定日期范围得到的行为数据
    val actionRDD = SparkUtils.getActionRDDByDateRange(spark,taskParam)

    // 生成Session粒度的基础数据，得到的数据格式为：<sessionId, actionRDD>
    //import spark.implicits._
    val sessionId2ActionRDD = actionRDD.map(row=>(row(2).toString,row))

    // 对于以后经常使用的基础数据，最好缓存起来，便于以后获取
    val sessionId2ActionRDDCache = sessionId2ActionRDD.cache()

    // 对行为数据进行聚合
    // 1、将行为数据按照sessionId进行分组
    // 2、行为数据RDD（actionRDD）需要和用户信息进行join，这样就得到了session粒度的明细数据
    //  明细数据包含：session对应的用户基本信息
    // 生成的数据格式为：<sessionId, (sessionId,searchKeywords,clickCategoryIds,age,professional,sex)>
    //    sessionId2ActionRDDCache.groupByKey()
    val sessionId2AggInfoRDD = aggregateBySession(sc,spark,sessionId2ActionRDD)
    // 实现Accumulator累加器对数据字段值的累加
    // val sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator)
    val sessionAggrStatAccumulator = new SessionAggrStatAccumulator
    sc.register(sessionAggrStatAccumulator)

    // 以session粒度的数据进行聚合，需要按照使用者指定的筛选条件进行筛选
    val filteredSessionId2AggrInfoRDD = filteredSessionAndAggrStat(sessionId2AggInfoRDD,taskParam,sessionAggrStatAccumulator)

    // 把按照使用者的条件过滤后的数据进行缓存
    filteredSessionId2AggrInfoRDD.persist(StorageLevel.MEMORY_AND_DISK)

    val value = getSessionId2DetailRDD(filteredSessionId2AggrInfoRDD, sessionId2ActionRDD)
    // 生成一个公共的RDD，通过筛选条件过滤出来的Session（filteredSessionId2AggrInfoRDD）来得到访问明细
    val sessionId2DetailRDD =
      value
    // 缓存通过筛选条件顾虑出来的Session得到的访问明细数据
    sessionId2DetailRDD.persist(StorageLevel.MEMORY_AND_DISK)
    // 如果将上一个聚合的需求统计的结果是无法进行存储的，因为没有调用一个action类型的算子
    // 所有必须再触发一个action算子后才能从累加器中获取到结果数据
    System.out.println(sessionId2DetailRDD.count)

    // 计算出各个范围的session占比，并存入数据库中
//    calcuaterAndPersistAggrStat(sessionAggrStatAccumulator.value,task.getTaskid)

    // 按时间比例随机抽取session
    // 1、计算出每小时的session数量
    // 2、计算出每小时的session数量与一天session数据量的占比
    // 3、按照比例进行随机抽取
//    randomExtractSession(sc,task.getTaskid,filteredSessionId2AggrInfoRDD,sessionId2DetailRDD)


    // top10热门品类
    // 1、首先获取到通过筛选条件的session访问过的所有品类
    // 2、计算出session访问过的所有品类的点击、下单、支付次数，用到了join
    // 3、开始实现自定义排序
    // 4、将品类的点击下单支付次数封装到自定义排序key中
    // 5、使用sortByKey进行自定义排序（降序排序）
    // 6、获取排序后的前10个品类
    // 7、将top10热门品类的每个品类的点击下单支付次数写入数据库
    val top10CategoryList = getTop10Category(task.getTaskid, sessionId2DetailRDD)



    // 获取top10活跃session
    // 1、获取到符合筛选条件的session明细数据
    // 2、按照session粒度的数据进行聚合，获取到session对应的每个品类的点击次数
    // 3、按照品类id，分组取top10，并且获取到top10活跃session
    // 4、结果的存储
    getTop10Session(top10CategoryList, sessionId2DetailRDD, task.getTaskid, sc)




    spark.stop()

  }

  /**
    * 对行为数据做session粒度的聚合
    * @param sc spark-core的上下文对象
    * @param spark  spark-sql的上下文对象
    * @param sessionId2ActionRDD  行为数据
    * @return
    */
  private def aggregateBySession(sc: SparkContext, spark: SparkSession, sessionId2ActionRDD: RDD[(String, Row)]) = {
    // 首先对行为数据进行分组
    val sessionId2ActionPairRDD = sessionId2ActionRDD.groupByKey

    // 对每个session分组进行聚合，将session中所有的搜索关键字和点击品类都聚合起来
    // 格式：<userId, partAggrInfo(sessionId, searchKeywords, clickCategoryIds, visitLength, stepLength, startTime)>

    val userId2PartAggrInfoRDD: RDD[(Long, String)] = sessionId2ActionPairRDD.map(tup => {
      val sessionId = tup._1
      val it = tup._2.iterator

      // 用来存储搜索关键字和点击品类
      val searchKeywordsBuffer = new StringBuffer
      val clickCategoryIdsBuffer = new StringBuffer

      // 用来存储userid
      var userId = 0L

      // 用来存储开始时间和结束时间
      var startTime:Date = null
      var endTime:Date = null

      // 用来存储session的访问步长
      var stepLength = 0
      while (it.hasNext) {
        // 提取每个访问行为的搜索关键字
        val row: Row = it.next

        userId = row.getLong(1)
        // 注意：如果该行为数据属于搜索行为，searchKeyword是有值的
        //  如果该行为数据是点击品类行为，clickCategoryId是有值的
        //  但是，任何的一个行为，不可能两个字段都有值

        val searchKeyWord = row.getString(5)
        val clickCategoryIds = String.valueOf(row.getAs[Long](6))

        // 追加搜索关键字
        if (!StringUtils.isEmpty(searchKeyWord)) if (!searchKeywordsBuffer.toString.contains(searchKeyWord)) searchKeywordsBuffer.append(searchKeyWord + ",")

        // 追加相关品类
        if (!StringUtils.isEmpty(clickCategoryIds)) if (!clickCategoryIdsBuffer.toString.contains(clickCategoryIds)) clickCategoryIdsBuffer.append(clickCategoryIds + ",")

        // 计算session开始时间和结束时间
        val actionTime = DateUtils.parseTime(row.getString(4))
        if (startTime==null) startTime = actionTime
        if (endTime==null) endTime = actionTime
        if (actionTime.before(startTime)) startTime = actionTime
        if (actionTime.after(endTime)) endTime = actionTime

        // 计算访问步长
        stepLength += 1
      }

      // 截取搜索关键字和点击品类的两端的","
      val searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString)
      val clickCategory = StringUtils.trimComma(clickCategoryIdsBuffer.toString)

      // 计算访问时长，单位秒
      val visitLenth = (endTime.getTime - startTime.getTime) / 1000

      // 聚合数据，数据以字符串拼接的方式:key=value|key=value
      val partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" +
        Constants.FIELD_CLICK_CATEGORY_IDS + "+" + clickCategory + "|" + Constants.FIELD_VISIT_LENGTH + "=" + visitLenth + "|" +
        Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)+"|"
      (userId, partAggrInfo)


    })

      // 查询出所有用户数据，映射成<userId,Row>
      val sql = "select * from user_info"
      val userInfoRDD = spark.sql(sql).rdd
      val userId2InfoRDD = userInfoRDD.map(userrow=>(userrow.getLong(0),userrow))

      // 将session粒度的聚合数据（userId2PartAggrInfoRDD）和用户信息进行join，
      // 生成的格式为：<userId, <sessionInfo, userInfo>>
      val userId2FullInfoRDD = userId2PartAggrInfoRDD.join(userId2InfoRDD)

      // 对join后的数据进行拼接，并返回<sessionId, fullAggrInfo>格式的数据
      val sessionId2FullAggrInfoRDD = userId2FullInfoRDD.map(tup=>{
        val partAggrInfo = tup._2._1
        // 获取用户信息
        val userInfoRow = tup._2._2
        // 获取sessionid
        val sessionId = StringUtils.getFieldFromConcatString(partAggrInfo,"\\|",Constants.FIELD_SESSION_ID)

        // 获取用户信息的age
        val age = userInfoRow.getInt(3)
        // 获取用户信息的职业
        val professional = userInfoRow.getString(4)
        // 获取用户信息中的城市
        val city = userInfoRow.getString(5)
        // 获取用户信息中的性别
        val sex = userInfoRow.getString(6)

        // 拼接
        val fullAggrInfo = partAggrInfo+Constants.FIELD_AGE+"="+age+"|"+Constants.FIELD_PROFESSIONAL+"="+professional+"|+" +
          Constants.FIELD_CITY+"="+city+"|"+Constants.FIELD_SEX+"="+sex+"|"
        (sessionId,fullAggrInfo)
      })
      sessionId2FullAggrInfoRDD
    }




  /**
    * 按照使用者提供的条件过滤session数据并进行聚合
    *
    * @param sessionId2AggInfoRDD 基础数据
    * @param taskParam 过滤条件
    * @param sessionAggrStatAccumulator 累加器
    */

  private def filteredSessionAndAggrStat(sessionId2AggInfoRDD: RDD[(String,String)], taskParam: JSONObject, sessionAggrStatAccumulator: SessionAggrStatAccumulator) = {
    // 获取使用者提交的条件
    val startAge = ParamUtils.getParam(taskParam,Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam,Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam,Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam,Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam,Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam,Constants.PARAM_KEYWORDS)
    val categorys = ParamUtils.getParam(taskParam,Constants.PARAM_CATEGORY_IDS)

    // 拼接
    var _paramter = (if(startAge != null) Constants.PARAM_START_AGE+"="+startAge+"|" else "") +
      (if(endAge != null) Constants.PARAM_END_AGE+"="+endAge+"|" else "") +
      (if(professionals != null) Constants.PARAM_PROFESSIONALS+"="+professionals+"|" else "") +
      (if(cities != null) Constants.PARAM_CITIES+"="+cities+"|" else "") +
      (if(sex != null) Constants.PARAM_SEX+"="+sex+"|" else "") +
      (if(keywords != null) Constants.PARAM_KEYWORDS+"="+keywords+"|" else "") +
      (if(categorys != null) Constants.PARAM_CATEGORY_IDS+"="+categorys+"|" else "")

    // 把_paramter的值的末尾的"|"截取掉
    if (_paramter.endsWith("|")) _paramter = _paramter.substring(0, _paramter.length - 1)
    val parameter = _paramter

    // 初始化点击时长和步长
    val visitLength = 0L
    val stepLength = 0L

    // 根据筛选条件进行过滤和聚合
    val filteredSessionAggrInfoRDD = sessionId2AggInfoRDD.filter(filterCondition(_,parameter,sessionAggrStatAccumulator))

    filteredSessionAggrInfoRDD


  }

  /**
    * 按照使用者条件进行过滤
    * @param tuple  基础数据
    * @param parameter  过滤条件
    * @param sessionAggrStatAccumulator 累加器
    * @return
    */
  private def filterCondition(tuple: (String, String), parameter: String, sessionAggrStatAccumulator: SessionAggrStatAccumulator): Boolean = {
    // 从tup中拿到基础数据
    val aggrInfo = tuple._2

    /**
      * 依次按照筛选条件进行过滤
      */
    // 按照年龄条件进行过滤
    if (!ValidUtils.between(aggrInfo,Constants.FIELD_AGE,parameter,Constants.PARAM_START_AGE,Constants.PARAM_END_AGE)) return false
    // 按照职业进行过滤
    if (!ValidUtils.in(aggrInfo,Constants.FIELD_PROFESSIONAL,parameter,Constants.PARAM_PROFESSIONALS)) return false
    // 按照城市进行过滤
    if (!ValidUtils.in(aggrInfo,Constants.FIELD_CITY,parameter,Constants.PARAM_CITIES)) return false
    // 按照性别进行过滤
    if (!ValidUtils.in(aggrInfo,Constants.FIELD_SEX,parameter,Constants.PARAM_SEX)) return false
    // 按照关键字进行过滤
    if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) return false
    // 按照点击品类进行过滤
    if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) return false

    /**
      * 代码执行到这里，说明该session通过了用户指定的筛选提条件
      * 接下来要对session的访问时长和访问步长进行统计
      */

    sessionAggrStatAccumulator.add(Constants.SESSION_COUNT)

    // 根据session对应的时长和步长的时间范围进行累加操作
    val visitLength = (StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_VISIT_LENGTH)).toLong
    val stepLength = (StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_STEP_LENGTH)).toLong

    // 计算访问时长范围
    if(visitLength>=1&&visitLength<=3) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    else if (visitLength>=4&&visitLength<=6) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    else if (visitLength>=7&&visitLength<=9) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    else if (visitLength>=10&&visitLength<30) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    else if (visitLength>=30&&visitLength<60) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    else if (visitLength>=60&&visitLength<180) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    else if (visitLength>=180&&visitLength<600) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    else if (visitLength>=600&&visitLength<1800) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    else if (visitLength>=1800) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m)

    // 计算访问步长范围
    if (stepLength >= 1&& stepLength<=3) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3)
    else if (stepLength >= 4&& stepLength<=6) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6)
    else if (stepLength >= 7&& stepLength<=9) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9)
    else if (stepLength >= 10&&stepLength<30) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30)
    else if (stepLength >= 30&& stepLength<60) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60)
    else if (stepLength >= 60) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60)

    true

  }

  /**
    * 获取通过筛选条件的session访问明细
    * @param filteredSessionAggrInfoRDD
    * @param sessionId2ActionRDD
    * @return
    */
  private def getSessionId2DetailRDD(filteredSessionAggrInfoRDD: RDD[(String, String)], sessionId2ActionRDD: RDD[(String, Row)]) = {
    // 过滤后的数据和访问明细数据进行join
    val sessionId2DetailRDDTmp = filteredSessionAggrInfoRDD.join(sessionId2ActionRDD)
    // 聚合数据
    val sesionId2DetailRDD = sessionId2DetailRDDTmp.map(tup=>(tup._1,tup._2._2))
    sesionId2DetailRDD
   }

  /**
    * 计算出各个范围的session占比，并存入数据库
    * @param value
    * @param taskid
    */
  private def calcuaterAndPersistAggrStat(value: String, taskid: Long) = {
    // 首先从Accumulator统计的字符串中获取各个聚合的值
    val session_count = (StringUtils.getFieldFromConcatString(value,"\\|",Constants.SESSION_COUNT)).toLong
    val visit_length_1s_3s = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s)).toLong
    val visit_length_4s_6s = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s)).toLong
    val visit_length_7s_9s = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s)).toLong
    val visit_length_10s_30s = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s)).toLong
    val visit_length_30s_60s = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s)).toLong
    val visit_length_1m_3m = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m)).toLong
    val visit_length_3m_10m = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m)).toLong
    val visit_length_10m_30m = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m)).toLong
    val visit_length_30m = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m)).toLong
    val step_length_1_3 = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3)).toLong
    val step_length_4_6 = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6)).toLong
    val step_length_7_9 = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9)).toLong
    val step_length_10_30 = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30)).toLong
    val step_length_30_60 = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60)).toLong
    val step_length_60 = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60)).toLong

    // 计算访问时长和访问步长的范围占比
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s.toDouble / session_count.toDouble, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s.toDouble / session_count.toDouble, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s.toDouble / session_count.toDouble, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s.toDouble / session_count.toDouble, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s.toDouble / session_count.toDouble, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m.toDouble / session_count.toDouble, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m.toDouble / session_count.toDouble, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m.toDouble / session_count.toDouble, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m.toDouble / session_count.toDouble, 2)
    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3.toDouble / session_count.toDouble, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6.toDouble / session_count.toDouble, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9.toDouble / session_count.toDouble, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30.toDouble / session_count.toDouble, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60.toDouble / session_count.toDouble, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60.toDouble / session_count.toDouble, 2)


    // 将统计结果存入数据库中
    val sessionAggrStat = new SessionAggrStat
    sessionAggrStat.setTaskid(taskid)
    sessionAggrStat.setSession_count(session_count)
    sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio)
    sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio)
    sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio)
    sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio)
    sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio)
    sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio)
    sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio)
    sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio)
    sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio)
    sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio)
    sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio)
    sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio)
    sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio)
    sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio)
    sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio)

    val sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO
    sessionAggrStatDAO.insert(sessionAggrStat)

  }

  /**
    * 按时间比例随机抽取session
    * @param sc
    * @param taskid
    * @param filteredSessionId2AggrInfoRDD
    * @param sessionId2DetailRDD
    * @return
    */
  private def randomExtractSession(sc: SparkContext, taskid: Long, filteredSessionId2AggrInfoRDD: RDD[(String, String)], sessionId2DetailRDD: RDD[(String, Row)]): Unit = {
    /**
      * 第一步：计算出每个小时的session数量
      */
    // 首先把数据格式调整为：<date_hour, data>
    val time2SessionIdRDD = filteredSessionId2AggrInfoRDD.map(tup => {
      // 获取聚合数据
      val aggrInfo = tup._2
      // 从聚合数据里获取startTime
      val startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME)
      // 获取startTime中的日期和时间
      val dateHour = DateUtils.getDateHour(startTime)

      (dateHour, aggrInfo)
    })

    // 要得到每天每小时的session数量，然后计算出每天每小时session抽取索引，遍历每天每小时的session
    // 首先抽取出session聚合数据，写入数据库表：session_random_extract
    // time2SessionIdRDD的value值是每天某个小时的session聚合数据
    // 计算出每天每小时的session数量
    val countMap = time2SessionIdRDD.countByKey

    /**
      * 第二步：使用时间比例随机抽取算法，计算出每天每小时抽取的session索引
      */
    // 将countMap数据的格式<date_hour, data>转换为：<yyyy-MM-dd, <HH, count>>并放到Map里
    val dateHourCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()

    // 将数据循环地放到dateHourCountMap
    for (countelem <- countMap) {
      // 取出日期和小时
      val dateHour = countelem._1
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)

      // 取出每个小时的count数
      val count = (String.valueOf(countelem._2)).toLong
      // 用来存储<hour,count>
      var hourCountMap = new mutable.HashMap[String, Long]()
      hourCountMap = dateHourCountMap.get(date).getOrElse(null)
      if (hourCountMap == null) {
        hourCountMap = new mutable.HashMap[String, Long]()
        dateHourCountMap.put(date, hourCountMap)
      }
      hourCountMap.put(hour, count)
    }
    // 实现按时间比例抽取算法
    // 比如要抽取100个session，先按照天进行平均
    val extranctNumbder = 100 / dateHourCountMap.size

    // Map<date,Map<hour,List(5,4,34...)>>
    val dateHourExtractMap = new mutable.HashMap[String,mutable.HashMap[String,ListBuffer[Int]]]()

    val random = new Random()

    for (dateCountElem <- dateHourCountMap) {

      // 获得日期
      val date = dateCountElem._1
      // 获取到日期对应的小时和count数
      val hourCountMap = dateCountElem._2

      // 计算出当前一天的session总数
      var sessionCount = 0L
      for (hourCount <- hourCountMap.values) {
        sessionCount += hourCount
      }

      // 把一天的session数量put到dateHourExtractMap
      var hourExtractMap = dateHourExtractMap.get(date).getOrElse(null)
      if (hourExtractMap == null){
        hourExtractMap = new mutable.HashMap[String,ListBuffer[Int]]
        dateHourExtractMap.put(date,hourExtractMap)
      }

      // 遍历每个小时获取每个小时的session数量
      for (hourCountElem <- hourCountMap) {
        val hour = hourCountElem._1 // 小时
        val count = hourCountElem._2 // 小时对应的个数

        // 计算每小时session数量占当天session数量的比例
        // 最后计算出当前小时需要抽取的session数量
        var hourExtractNumber = ((count.toDouble / sessionCount.asInstanceOf[Double]) * extranctNumbder).toInt
        // 当前需要抽取的session数量有可能大于每小时的session数量
        // 让当前小时需要抽取的session数量直接等于每小时的session数量
        if (hourExtractNumber > count) {
          hourExtractNumber = count.toInt
        }

        // 后去当前小时存放随机数的list，如果没有就创建一个
        var extractIndexList = new ListBuffer[Int]
        extractIndexList = hourExtractMap.get(hour).getOrElse(null)
        if (extractIndexList == null) {
          extractIndexList = new ListBuffer[Int]
          hourExtractMap.put(hour, extractIndexList)
        }

        // 将上面计算出的随机数，用while判断生成的随机数不能是重复的
        for (i <- 0 until hourExtractNumber) {
          var extractIndex = random.nextInt(count.asInstanceOf[Int])
          while (extractIndexList.contains(extractIndex)) {
            // 如果有重复的随机索引，就重新生成随机数
            extractIndex = random.nextInt(count.asInstanceOf[Int])
          }
          extractIndexList += extractIndex

        }
      }
    }

    // 把dateHourExtractMap封装到fastUtilDateHourExtractMap
    // fastUtil可以封装Map、List、Set，相比较普通的Map、List、Set占用的内存更小，
    // 所以传输的速度更快，占用的网络IO更少
    val fastUtilDateHourExtractMap = new mutable.HashMap[String, mutable.HashMap[String, IntList]]();
    for (dateHourExtractEntry <- dateHourExtractMap.entrySet) {
      // date
      val date = dateHourExtractEntry.getKey
      // <hour, extract>
      val hourExtractMap = dateHourExtractEntry.getValue
      // 存储<hour, extract>数据
      val fastUtilHourExtractMap = new mutable.HashMap[String, IntList]()
      for (hourExtractEntry <- hourExtractMap.entrySet) {
        // hour
        val hour = hourExtractEntry.getKey
        // extract
        val extractList = hourExtractEntry.getValue
        // 封装
        val fastUtilExtractList = new IntArrayList();
        for (i <- 0 until extractList.size) {
          fastUtilExtractList.add(extractList.get(i))
        }

        fastUtilHourExtractMap.put(hour, fastUtilExtractList)
      }

      fastUtilDateHourExtractMap.put(date,fastUtilHourExtractMap)
    }

    /**
      * 在集群执行任务的时候，有可能多个Executor会远程的获取上面的Map的值
      * 这样会产生大量的网络IO，此时最好用广播变量把该值广播到每一个参与计算的Executor
      */
    val dateHourExtractMapBroadcast = sc.broadcast(fastUtilDateHourExtractMap)

    /**
      * 第三步：遍历每天每小时的session，根据随机索引进行抽取
      */
    // 获取到聚合数据，数据结构为：<dateHour, (session, aggInfo)>
    val time2SessionRDD = time2SessionIdRDD.groupByKey

    // 用flatMap，遍历所有的time2SessionRDD
    // 然后遍历每天每小时的session
    // 如果发现某个session正好在指定的这天这个小时的随机抽取索引上
    // 将该session写入到session_random_extract表中
    // 接下来再将抽取出来的session返回，生成一个新的JavaRDD<String>
    // 用抽取出来的sessionId去join访问明细，并写入数据库表session_detail
    val extractSessionIdsRDD = time2SessionRDD.flatMap(tup => {

      // 用来存储<sessionId, sessionId>
      val extractSessionIds = new util.ArrayList[Tuple2[String, String]]
      val dateHour = tup._1
      val date = dateHour.split("_")(0)

      // 日期
      val hour = dateHour.split("_")(1)

      // 小时对应的随机抽取索引信息
      val it = tup._2.iterator

      // 获取广播过来的值
      val dateHourExtractMap = dateHourExtractMapBroadcast.value

      // 获取这个小时对应的抽取索引List
      val extractIndexList = dateHourExtractMap.get(date).get(hour)
      val sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO
      var index = 0
      while (it.hasNext) {
        val sessionAggrInfo = it.next
        if (extractIndexList.contains(index)) {
          val sessionId = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID)

          // 将数据存入数据库
          val sessionRandomExtract = new SessionRandomExtract
          sessionRandomExtract.setTaskid(taskid)
          sessionRandomExtract.setSessionid(sessionId)
          sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_START_TIME))
          sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS))
          sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS))
          sessionRandomExtractDAO.insert(sessionRandomExtract)

          // 将sessionId加入List
          extractSessionIds.add(new Tuple2[String, String](sessionId, sessionId))
        }

        index += 1
      }

      extractSessionIds
    })


    /**
      * 第四步：获取抽取出来的session对应的明细数据写入数据库session_detail
      */
    // 把明细join进来
    val extractSessionDetailRDD = extractSessionIdsRDD.join(sessionId2DetailRDD)
    extractSessionDetailRDD.foreachPartition(partition=>{
      // 用来存储明细数据
      val sessionDetails = new util.ArrayList[SessionDetail]()

      // 开始封装值
      partition.foreach(tup=>{
        val row = tup._2._2
        val sessionDetail = new SessionDetail
        sessionDetail.setTaskid(taskid)
        sessionDetail.setUserid(row.getLong(1))
        sessionDetail.setSessionid(row.getString(2))
        sessionDetail.setPageid(row.getLong(3))
        sessionDetail.setActionTime(row.getString(4))
        sessionDetail.setSearchKeyword(row.getString(5))
        sessionDetail.setClickCategoryId(row.getAs[Long](6))
        sessionDetail.setClickProductId(row.getAs[Long](7))
        sessionDetail.setOrderCategoryIds(row.getString(8))
        sessionDetail.setOrderProductIds(row.getString(9))
        sessionDetail.setPayCategoryIds(row.getString(10))
        sessionDetail.setPayProductIds(row.getString(11))
        sessionDetails.add(sessionDetail)
      })

      // 结果存储到mysql中
      val sessionDetailDAO = DAOFactory.getSessionDetailDAO
      sessionDetailDAO.insertBatch(sessionDetails)
    })
  }



  /**
    * 计算top10热门品类
    *
    * @param taskid
    * @param sessionId2DetailRDD
    */
  def getTop10Category(taskid: Long, sessionId2DetailRDD: RDD[(String, Row)]) = {
    // 第一步：获取通过筛选条件的session访问过的所有品类
    // 获取session访问过的所有品类id（访问过指的是点击过、下单过、支付过）
    var categoryIdRDD = sessionId2DetailRDD.flatMap(tup=>{
      // 拿到行为数据
      val row = tup._2

      // 用来存储点击、下单、支付的所有品类id信息
      val list = new util.ArrayList[(Long,Long)]

      // 添加点击品类
      if (!row.isNullAt(6)){
        val clickCategoryId = row.getLong(6)
        list.add(new Tuple2[Long,Long](clickCategoryId,clickCategoryId))
      }
      // 添加下单信息
      val orderCategoryIds = row.getString(8)
      if (orderCategoryIds != null) {
        val orderCategoryIdsSplited = orderCategoryIds.split(",")
        for (orderCategoryId <- orderCategoryIdsSplited) {
          val longOrderCategoryId = (orderCategoryId).toLong
          list.add(new Tuple2[Long, Long](longOrderCategoryId, longOrderCategoryId))
        }
      }

      // 添加支付信息
      val payCategoryIds = row.getString(10)
      if (payCategoryIds != null) {
        val payCategoryIdsSplited = payCategoryIds.split(",")
        for (payCategoryId <- payCategoryIdsSplited) {
          val longPayCategoryId = (payCategoryId).toLong
          list.add(new Tuple2[Long, Long](longPayCategoryId, longPayCategoryId))
        }
      }


      list
    })
    /**
      * session访问过的所有品类中，可能有重复的categoryId，需要去重
      * 如果不去重，在排序过程中会对categoryId重复排序，最后会产生重复的数据
      */
    categoryIdRDD = categoryIdRDD.distinct

    /**
      * 第二步：计算出各品类的点击、下单、支付次数
      *
      */

    // 计算各品类的点击次数
    val clickCategoryId2CountRDD = getClickCategoryId2CountRDD(sessionId2DetailRDD)

    // 计算各品类的下单次数
    val orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(sessionId2DetailRDD)

    // 计算各品类的支付次数
    val payCategoryId2CountRDD = getPayCategoryId2CountRDD(sessionId2DetailRDD)
    /**
      * 第三步：join各品类的点击、下单、支付次数
      * categoryIdRDD数据里面，包含了所有符合条件的并且是过滤掉重复品类的session
      * 在第二步中分别计算了点击下单支付次数，可能不是包含所有品类的
      * 比如：有的品类只是有点击过，但没有下单，类似的这种情况有很多
      * 所以，在这里如果要join，就不能用join，需要用leftOuterJoin
      */

    val categoryId2CountRDD = joinCategoryAndDetail(categoryIdRDD,clickCategoryId2CountRDD,orderCategoryId2CountRDD,payCategoryId2CountRDD)
    /**
      * 第四步： 自定义排序---CategorySortKey
      */

    /**
      * 第五步：将数据映射为: <CategorySortKey, countInfo>格式的RDD，再进行二次排序
      */

    val sortCountRDD = categoryId2CountRDD.map(tup=>{
      val countInfo = tup._2

      val clickCount = (StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CLICK_COUNT)).toLong
      val orderCount = (StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_ORDER_COUNT)).toLong
      val payCount = (StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_PAY_COUNT)).toLong

      // 创建自定义排序
      val sortKey = new CategorySortKey(clickCount, orderCount, payCount)

      (sortKey, countInfo)
    })

    // 进行降序排序
    val sortedCategoryCountRDD = sortCountRDD.sortByKey(false)

    /**
      * 第六步：用take(10)取出top10热门品类，写入数据库
      */
    val top10CategoryList = sortedCategoryCountRDD.take(10)
    // 将结果进行封装和存储
    val top10CategoryDAO = DAOFactory.getTop10CategoryDAO
    for (tup <- top10CategoryList) {
      val countInfo = tup._2

      val categoryId = (StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID)).toLong
      val clickCount = (StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT)).toLong
      val orderCount = (StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT)).toLong
      val payCount = (StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT)).toLong

      val top10Category = new Top10Category
      top10Category.setTaskid(taskid)
      top10Category.setCategoryid(categoryId)
      top10Category.setClickCount(clickCount)
      top10Category.setOrderCount(orderCount)
      top10Category.setPayCount(payCount)

      top10CategoryDAO.insert(top10Category)

    }
    top10CategoryList
  }

  /**
    * 计算各品类的点击次数
    * @param sessionId2DetailRDD
    */
  def getClickCategoryId2CountRDD(sessionId2DetailRDD: RDD[(String, Row)]) = {
    // 过滤掉单字段为空的数据
    val clickActionRDD = sessionId2DetailRDD.filter(tup=>{
      val row = tup._2
      if (row.get(6)!=null) true
      else false
    })

    // 将每一个点击品类后面跟一个1，生成一个元组(clickCategoryId, 1)，为做聚合做准备
    val clickCategoryIdRDD = clickActionRDD.map(tup=>(tup._2.getLong(6),1L))

    // 计算各个品类的点击次数
    val clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(_+_)

    clickCategoryId2CountRDD
  }

  /**
    * 计算各品类的下单次数
    * @param sessionId2DetailRDD
    * @return
    */
  def getOrderCategoryId2CountRDD(sessionId2DetailRDD: RDD[(String, Row)]) = {
    // 过滤掉字段值为空的数据
    val orderActionRDD = sessionId2DetailRDD.filter(tup=>{
      // 过滤掉单字段为空的数据
      val row = tup._2
      if (row.getString(8) !=null) true
      else false
    })

    // 生成元组便于聚合
    val orderCategoryIdRDD = orderActionRDD.flatMap(tup=>{
      val orderCategoryIds = tup._2.getString(8)
      val orderCategoryIdsSplited: Array[String] = orderCategoryIds.split(",")
      // 用来存储切分后的数据:(orderCategoryId, 1L)
      val list = new util.ArrayList[(Long,Long)]
      for (orderCategoryId <- orderCategoryIdsSplited) {
        list.add((orderCategoryId.toLong, 1L))
      }

      list
    })

    // 进行聚合
    val orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey(_+_)
    orderCategoryId2CountRDD
  }

  /**
    * 计算各品类的支付次数
    * @param sessionId2DetailRDD
    * @return
    */
  def getPayCategoryId2CountRDD(sessionId2DetailRDD: RDD[(String, Row)]) = {
    // 过滤支付字段值为空的数据
    val payActionRDDD = sessionId2DetailRDD.filter(tup => {
      val row = tup._2
      if (row.getString(10) != null) true
      else false
    })

    // 把过滤后的数据生成一个个元组，便于以后聚合
    val payCategoryIdRDD = payActionRDDD.flatMap(tup => {
      val payCategoryIds = (tup._2).getString(10)
      val payCategoryIdsSplited = payCategoryIds.split(",")
      // 用于存储切分后的数据：(orderCategoryId, 1L)
      val list = new util.ArrayList[(Long, Long)]
      for (payCategoryId <- payCategoryIdsSplited) {
        list.add((payCategoryId.toLong, 1L))
      }

      list
    })

    // 进行聚合
    val payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey(_ + _)

    payCategoryId2CountRDD
  }

  /**
    * 连接品类RDD与数据RDD
    *
    * @param categoryIdRDD            session访问过的所有品类id
    * @param clickCategoryId2CountRDD 各品类的点击次数
    * @param orderCategoryId2CountRDD 各品类的下单次数
    * @param payCategoryId2CountRDD   各品类的支付次数
    * @return
    */
  def joinCategoryAndDetail(categoryIdRDD: RDD[(Long, Long)], clickCategoryId2CountRDD: RDD[(Long, Long)], orderCategoryId2CountRDD: RDD[(Long, Long)], payCategoryId2CountRDD: RDD[(Long, Long)]) = {

    // 注意：如果用leftOuterJoin，就可能出现右边RDD中join过来的值为空的情况
    // 所以tuple中的第二个值用Optional<Long>类型，代表可能有值，也可能没有值
    val tmpJoinRDD = categoryIdRDD.leftOuterJoin(clickCategoryId2CountRDD)

    // 把数据格式调整为:(categoryId,"categoryId=品类|clickCount=点击次数"
    var tmpMapRDD = tmpJoinRDD.map(tup=>{
      val categoryId = tup._1
      val optional = tup._2._2
      var clickCount = optional.getOrElse(0L)
      val value = Constants.FIELD_CATEGORY_ID+"="+categoryId+"|"+Constants.FIELD_CLICK_COUNT+"="+clickCount
      (categoryId,value)
    })

    // 再次与下单次数进行leftOuterJoin
    tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).map(tup => {
      val categoryId = tup._1
      var value = tup._2._1

      val optional = tup._2._2
      var orderCount = optional.getOrElse(null)
      if (orderCount == null)
        orderCount = 0L

      value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount

      (categoryId, value)
    })

    // 再次与支付次数进行leftOuterJoin
    tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).map(tup => {
      val categoryId = tup._1
      var value = tup._2._1

      val optional = tup._2._2
      var payCount = optional.getOrElse(null)
      if (payCount == null)
        payCount = 0L

      value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount

      (categoryId, value)
    })

    tmpMapRDD
  }



  /**
  * top10活跃session
 */
  def getTop10Session(top10CategoryList: Array[(CategorySortKey, String)], sessionId2DetailRDD: RDD[(String, Row)], taskid: Long, sc: SparkContext) ={
    /**
      * 第一步：将top10热门品类id(categoryList)转换为RDD
      * 生成的数据格式为：(categoryId, categoryId)
      */
    val top10CategoryListBuffer = new ListBuffer[(Long,Long)]
    for (elem <- top10CategoryList) {
      val categoryid = (StringUtils.getFieldFromConcatString(elem._2,"\\|",Constants.FIELD_CATEGORY_ID)).toLong
      top10CategoryListBuffer.add((categoryid,categoryid))
    }

    // 用并行化的方式转换为RDD
    val top10CategoryRDD = sc.parallelize(top10CategoryListBuffer.toList)

    /**
      * 第二步：计算出top10品类被各个session点击的次数
      */
    val top10CategorySessionClick = top10CategorySession(sessionId2DetailRDD,top10CategoryRDD)

    /**
      * 第三步：分组取topN算法，实现获取每个品类的top10活跃用户并写入数据库
      */
    val top10SessionRDD = calculateTop10Session(top10CategorySessionClick, taskid)
    /**
      * 第四步：获取top10活跃session的明细数据写入数据库
      */
    insertTop10SessionDetail(top10SessionRDD, sessionId2DetailRDD, taskid)
  }


  /**
    * 计算每个session点击某个品类的次数
    *
    * @param sessionId2DetailRDD
    * @param top10CategoryRDD
    * @return
    */
  def top10CategorySession(sessionId2DetailRDD: RDD[(String, Row)], top10CategoryRDD: RDD[(Long, Long)]) = {
    // 将明细数据以sessionId进行分组
    val sessionDetails = sessionId2DetailRDD.groupByKey

    // 将品类id对应的session和count生成为<category,"sessionId,count">
    val categorySessionClickCount = sessionDetails.flatMap(tup=>{
      val sessionid = tup._1
      val it = tup._2.iterator

      // 用来存储品类对应的点击次数<key=categoryId, value=次数>
      val categoryCount = new mutable.HashMap[Long,Long]()
      while (it.hasNext){
        val row = it.next()
        if(row.get(6)!=null){
          val categoryId = row.getLong(6)
          var count = 0L
          if(categoryCount.get(categoryId).getOrElse(null)!=null){
            count = categoryCount.get(categoryId).getOrElse(0L)
            count += 1
          }

          categoryCount.put(categoryId,count)
        }
      }

      // 返回一个结果List，格式为：<categoryId, "sessionId,count">
      val list = new ListBuffer[(Long,String)]
      for (elem <- categoryCount) {
        val categoryId = elem._1
        val count = elem._2
        val value = sessionid+","+count
        list += ((categoryId,value))
      }
      list
    })
    // 获取top10热门品类被各个session点击的次数：<categoryId, "sessionId,count">
    val top10CategorySessionCount = top10CategoryRDD.join(categorySessionClickCount).map(tup=>{
      (tup._1,tup._2._2)
    })

    top10CategorySessionCount
  }
  /**
    * 获取每个品类的top10活跃用户
    *
    * @param top10CategorySessionClick
    * @param taskid
    * @return
    */
  def calculateTop10Session(top10CategorySessionClick: RDD[(Long, String)], taskid: Long) = {
    // 以categoryId进行分组
    val top10CategorySessionCountsRDD = top10CategorySessionClick.groupByKey

    // 以每组top10session，分组取top10
    val top10Session = top10CategorySessionCountsRDD.flatMap(tup=>{
      val categoryId = tup._1
      val it = tup._2.iterator

      // 用来存储topN的排序数组
      val top10Sessions = new Array[String](10)

      while(it.hasNext){
        // "session,count"
        val sessionCount = it.next
        val count = sessionCount.split(",")(1).toLong
        // 遍历排序数组（topN算法)
        var i = 0
        import scala.util.control.Breaks._
        breakable{
          while(i < top10Sessions.length){
            // 判断，如果当前索引下的数据为null，就直接将sessioncount赋值给当前的i位数据
            if(top10Sessions(i) == null){
              top10Sessions(i) = sessionCount
              break
            }else{
              val _count = top10Sessions(i).split(",")(1).toLong
              // 判断，如果sessionCount比i位的sessionCount（_count）大，
              // 从排序数组最后一位开始，到i位，所有的数据往后挪一位
              if (count > _count){
                var j = 9
                while (j>i){
                  top10Sessions(j) = top10Sessions(j-1)
                  j -= 1
                }
                // 将sessionCount赋值给top10Sessions的i位数据
                top10Sessions(i) = sessionCount
                break
              }
            }
            i += 1
          }
        }
      }

      // 用来存储top10Sessions里的sessionId，格式为：<sessionId, sessionId>
      val list = new ListBuffer[(String,String)]
      // 将数据写入到数据库中
      for (elem <- top10Sessions) {
        if(elem != null){
          val sessionId = elem.split(",")(0)
          val count = elem.split(",")(1).toLong
          val top10Session = new Top10Session
          top10Session.setTaskid(taskid)
          top10Session.setSessionid(sessionId)
          top10Session.setCategoryid(categoryId)
          top10Session.setClickCount(count)

          val top10SessionDAO = DAOFactory.getTop10SessionDAO
          top10SessionDAO.insert(top10Session)
          list += ((sessionId,sessionId))
        }
      }
      list

    })
    top10Session
  }

  /**
    * 将top10活跃session的明细数据写入数据库
    * @param top10SessionRDD
    * @param sessionId2DetailRDD
    * @param taskid
    * @return
    */
  def insertTop10SessionDetail(top10SessionRDD: RDD[(String, String)], sessionId2DetailRDD: RDD[(String, Row)], taskid: Long) ={
    // (String,(String,row))
    val sessionDetailsRDD = top10SessionRDD.join(sessionId2DetailRDD)
    sessionDetailsRDD.foreach(tup=>{
      // 获取session的明细数据
      val row = tup._2._2
      val sessionDetail = new SessionDetail
      sessionDetail.setTaskid(taskid)
      sessionDetail.setUserid(row.getLong(1))
      sessionDetail.setSessionid(row.getString(2))
      sessionDetail.setPageid(row.getLong(3))
      sessionDetail.setActionTime(row.getString(4))
      sessionDetail.setSearchKeyword(row.getString(5))
      if (!row.isNullAt(6)) {
        sessionDetail.setClickCategoryId(row.getLong(6))
      }
      if (!row.isNullAt(7)) {
        sessionDetail.setClickProductId(row.getLong(7))
      }
      sessionDetail.setOrderCategoryIds(row.getString(8))
      sessionDetail.setOrderProductIds(row.getString(9))
      sessionDetail.setPayCategoryIds(row.getString(10))
      sessionDetail.setPayProductIds(row.getString(11))

      val sessionDetailDAO = DAOFactory.getSessionDetailDAO
      sessionDetailDAO.insert(sessionDetail)
    })
  }



}
