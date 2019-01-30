package spark.page


import com.alibaba.fastjson.{JSON, JSONObject}
import constant.Constants
import dao.factory.DAOFactory
import domain.PageSplitConvertRate
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import spark.util.SparkUtils
import test.MockData
import util.{DateUtils, NumberUtils, ParamUtils}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object PageOneStepConvetRete {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(Constants.SPARK_APP_NAME_PAGE)
    SparkUtils.setMaster(conf)
    val sc = new SparkContext(conf)
    val spark = SparkUtils.getSparkSession()

    //生成模拟数据
    MockData.mock(sc, spark)

    // 查询任务，获取任务信息参数
    val taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE)
    val taskDAO = DAOFactory.getTaskDAO
    val task = taskDAO.findById(taskId)
    if (task == null) System.out.println("您给的taskId臣妾找不到相应的数据")
    val taskParam = JSON.parseObject(task.getTaskParam)

    // 查询指定日期范围内的用户访问行为数据
    val actionRDD = SparkUtils.getActionRDDByDateRange(spark, taskParam)

    // 对用户访问行为数据做映射，将数据映射为<sessionId, 访问行为数据>session粒度的数据
    // 因为用户访问页面切片的生成，是基于每个session访问数据
    // 如果脱离了session，页面切片是没有意义的
    val sessionId2ActionRDD = actionRDD.map(row => (row.getString(2), row))

    // 按sessionId进行分组，得到每个会话的所有行为
    val groupedSessionId2ActionRDD = sessionId2ActionRDD.groupByKey

    // 计算每个session用户的访问轨迹1,2,3，=》页面切片
    // ("1_2",1)
    val pageSplitRDD = generatePageSplit(sc, groupedSessionId2ActionRDD, taskParam)

    // 获取切片的访问量
    // ("1_2",count)
    val pageSplitMap = pageSplitRDD.countByKey()

    // 获取起始页面的访问量
    val startPagePv = getStartPageVisit(groupedSessionId2ActionRDD,taskParam)
    // 计算目标页面的各个页面切片的转化率
    // Map<String, Double>: key=各个页面切片， value=页面切片对应的转化率
    val convertRateMap = computePageSplitConvertRate(taskParam, pageSplitMap, startPagePv)
    // 将结果保存到数据库（taskID，“1_2=0.88|2_3=0.55....”）
    insertConvertRateToDB(taskId, convertRateMap)

    sc.stop()
  }
  /**
    * 把页面切片对应的转化率存入数据库
    * @param taskId
    * @param convertRateMap
    */
  def insertConvertRateToDB(taskId: Long, convertRateMap: mutable.HashMap[String, Double]) = {
    // 存储页面流对应的切片和转换率
    val buffer = new StringBuffer

    for (convertRateElem <- convertRateMap) {
      // 获取切片
      val pageSplit = convertRateElem._1
      // 获取转换率
      val convetRete = convertRateElem._2
      // 拼接
      buffer.append(pageSplit+"="+convetRete+"|")

    }

    // 获取拼接好的切片和转换率
    // 截取掉最后的 "|"
    val convertRate = buffer.toString.substring(0, buffer.toString.length - 1)

    val pageSplitConvertRate = new PageSplitConvertRate
    pageSplitConvertRate.setTaskid(taskId)
    pageSplitConvertRate.setConvertRate(convertRate)

    val pageSplitConvertRateDAO = DAOFactory.getPageSplitConvertRateDAO
    pageSplitConvertRateDAO.insert(pageSplitConvertRate)
  }


  /**
    * 计算页面切片转化率
    * @param taskParam
    * @param pageSplitMap
    * @param startPagePv
    * @return
    */
  def computePageSplitConvertRate(taskParam: JSONObject, pageSplitMap: collection.Map[String, Long], startPagePv: Long) = {
    // 获取目标页面流
    val targetPages = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW).split(",")

    // 上一个切片的pv
    var lastPageSplitPV = 0L
    // 根据要计算的切片的访问率，计算每一个切片的转化率（即页面单跳转化率）
    // 用来存储转化率
    val convertRateMap = new mutable.HashMap[String,Double]()

    /**
      * 求转化率：
      * 如果页面流为：1,3,5,6
      * 第一个页面切片：1_3
      * 第一个页面的转化率：1_3的pv / 1的pv
      */
    // 通过while循环，获取目标页面流中的各个页面切片和访问量
    var i = 1
    while(i < targetPages.length){
      // 获取页面切片
      val targetPageSplit = targetPages(i - 1) + "_" + targetPages(i)
      // 获取每个页面切片对应的访问量
      val targetPageSplitPV = pageSplitMap.get(targetPageSplit).getOrElse(0L)

      // 初始转换率
      var convertRate = 0.0
      // 生成转换率
      if(i == 1){
        convertRate = NumberUtils.formatDouble(targetPageSplitPV.toDouble / startPagePv.toDouble, 2)
      }else{
        convertRate = NumberUtils.formatDouble(targetPageSplitPV.toDouble / lastPageSplitPV.toDouble, 2)
      }

      convertRateMap.put(targetPageSplit,convertRate)

      lastPageSplitPV = targetPageSplitPV

      i += 1
    }

    convertRateMap

  }

  /**
    * 获取页面流中起始页面pv
    * @param groupedSessionId2ActionRDD
    * @param taskParam
    * @return
    */
  def getStartPageVisit(groupedSessionId2ActionRDD: RDD[(String, Iterable[Row])], taskParam: JSONObject) = {
    // 拿到使用者提供的页面流
    val targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)

    // 从页面流中获取起始页面id
    val startPageId = targetPageFlow.split(",")(0).toLong
    val startPageRDD = groupedSessionId2ActionRDD.map(tup=>{
      // 用来存储每个session访问的起始页面id
      val list = new ListBuffer[Long]
      val it = tup._2.iterator
      while(it.hasNext){
        val row = it.next()
        val pageId = row.getLong(3)
        if(pageId == startPageId){
          list += pageId
        }
      }
      list
    })
    startPageRDD.count()
  }

  /**
    * 页面切片的生成和页面流匹配算法的实现
    * @param sc
    * @param groupedSessionId2ActionRDD
    * @param taskParam
    * @return
    */
  def generatePageSplit(sc: SparkContext, groupedSessionId2ActionRDD: RDD[(String, Iterable[Row])], taskParam: JSONObject) = {
    // 解析参数，拿到使用者指定的页面流
    val targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)

    // 把目标页面广播到相应executor端
    val targetPageFlowBroadCast = sc.broadcast(targetPageFlow)

    // 实现页面流匹配算法
    // 计算每一个session中符合条件的页面切片
    // (sessionid,iteraor(action))
    val pageSplit = groupedSessionId2ActionRDD.flatMap(tup=>{
      // 用来存储切片，格式：<split,1>
      val list = new ListBuffer[(String,Int)]

      // 获取当前session对应的行为数据
      val it = tup._2.iterator

      // 获取目标页面流
      val targetPages = targetPageFlowBroadCast.value.split(",")
      /**
        * 代码运行到这里，session的访问数据已经拿到了，
        * 但默认情况下并没有排序,
        * 在实现转化率的时候需要把数据按照时间顺序进行排序
        */

      // 把访问行为数据放到list里，便于排序
      val rows = new ListBuffer[Row]
      while (it.hasNext){
        rows += it.next()
      }

      // 按照时间把当前会话的所有行为进行排序
      implicit val keyOrder = new Ordering[Row] {
        override def compare(x: Row, y: Row): Int = {
          //"yyyy--mm--dd hh:mm:ss"
          val actionTime1 = x.getString(4)
          val actionTime2 = y.getString(4)
          val dateTime1 = DateUtils.parseTime(actionTime1)
          val dateTime2 = DateUtils.parseTime(actionTime2)

          (dateTime1.getTime - dateTime2.getTime).toInt
        }
      }
      val sortedRows = rows.sorted(keyOrder)

      /**
        * 生成页面切片，并和页面流进行匹配
        */
      import scala.util.control.Breaks._
      // 定义一个上一个页面的id
      var lastPageId = -1L
      // 注意：现在拿到的rows里的数据是其中一个sessionId对应的所有行为数据
      for (row <- sortedRows) {
        val pageId = row.getLong(4)
        breakable {
          if (lastPageId == -1L) {
            lastPageId = pageId
            break
          }

          /**
            * 生成一个页面切片
            * 比如该用户请求的页面是：1,3,4,7
            * 上次访问的页面id：lastPageId=1
            * 这次请求的页面是：3
            * 那么生成的页面切片为：1_3
            */
          val pageSplit = lastPageId + "_" + pageId

          // 判断当前切片是否在目标页面流中
          var i = 1
          while (i < targetPages.length) {
            // 比如说：使用者指定的页面流是：1,2，5，6
            // 遍历的时候，从索引1开始，也就是从第二个页面开始
            // 这样第一个页面切片就是1_2
            val targetPageSplit = targetPages(i - 1) + "_" + targetPages(i)
            if (pageSplit.equals(targetPageSplit)) {

              list += ((pageSplit, 1))
              break
            }
            i += 1
          }
          lastPageId = pageId

        }
      }
      list
    })
      pageSplit
  }

}
