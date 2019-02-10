package spark.ad

import java.util.Date

import scala.collection.JavaConversions._
import conf.ConfigurationManager
import constant.Constants
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spark.util.SparkUtils
import util.DateUtils
import java.util

import dao.factory.DAOFactory
import domain.{AdBlacklist, AdStat, AdUserClickCount}
import org.apache.spark.rdd.RDD

/**
  * 广告点击流量实时统计
  * 实现过程：
  * 1、实时的计算各batch中的每天各个用户对各广告的点击量
  * 2、实时的将每天各个用户对各广告点击次数写入数据库，采用实时更新的方式
  * 3、使用filter过滤出每天某个用户对某个广告点击超过100次的黑名单，更新到数据库
  * 4、使用transform原语操作，对每个batch RDD进行处理，实现动态加载黑名单生成RDD，
  * 然后进行join操作，过滤掉batch RDD黑名单用户的广告点击行为
  * 5、使用updateStateByKey操作，实时的计算每天各省各城市各广告的点击量，并更新到数据库
  * 6、使用transform结合SparkSQL统计每天各省份top3热门广告（开窗函数）
  * 7、使用窗口操作，对最近1小时的窗口内的数据，计算出各广告每分钟的点击量，实时更新到数据库
  */
object AdClickRealTimeStatSpark extends Serializable {



  def main(args: Array[String]): Unit = { // 模板代码
    val conf = new SparkConf()
      .setAppName(Constants.SPARK_APP_NAME_AD)
    SparkUtils.setMaster(conf)
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("hdfs://node01:9000/cp-20190124")

    // 设置用于请求kafka的配置信息
    val group = "group01"
    val kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS)
    val topicsSet = Set(kafkaTopics)
    val brokerList = ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST)
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokerList,
      "serializer.class" -> "kafka.serializer.StringDecoder",
      "group.id" -> group,
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )
    /**
      * 使用kafka低级api，不对offset进行管理
      */
    val message = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    // 动态生成黑名单并转换为RDD
    // 1、计算出每个batch中的每天每个用户对每个广告的点击量，并存入数据库
    // 2、依据上面的结果，对每个batch中按date、userId、adId聚合的数据都要遍历一遍
    //      查询对应累计的点击次数，如果超过了100次，就认为是黑名单用户，
    //      然后对黑名单用户进行去重并存储
    dynamicGenerateBlackList(message)
    //获取黑名单并转化成RDD
    @transient
    val sc = ssc.sparkContext
    val blackListRDD = getBlackListToRDD(sc)

    // 根据动态黑名单进行数据过滤
    val filteredAdRealTimeLogDStream = filteredByBlackList(blackListRDD, message)
    // 业务一：计算每天各省各城市各广告的点击流量实时统计
    // 生成的数据格式为：<yyyyMMdd_province_city_adId, clickCount>
    val adRealTimeStatDStream = calcuateRealTimeStat(filteredAdRealTimeLogDStream)
    // 业务二：统计每天各省top3热门广告
    calculateProvinceTop3Ad(adRealTimeStatDStream)

  }

  /**
    * 实现过滤黑名单机制
    *
    * @param message
    * @return
    */
  def filteredByBlackList(blackListRDD: RDD[(Long, Boolean)], message: InputDStream[(String, String)]) = {
    // 接收到原始的用户点击行为日志后，根据数据库黑名单，进行实施过滤
    // 使用transform将DStream中的每个batch RDD进行处理，转换为任意其它的RDD
    // 返回的格式为：<userId, tup>  tup=<offset,(timestamp province city userId adId)>
    val filteredAdRealTimeLogDStream = message.transform(rdd=>{
      // 将原始数据RDD映射为<userId, Tuple2<kafka-key, kafka-value>>
      val mappedRDD = rdd.map(tup=>{
        // 获取用户点击行为
        val log = tup._2

        // 切分，原始数据的格式为：<offset,(timestamp province city userId adId)>
        val logSplited = log.split(" ")
        val userId = logSplited(3).toLong

        (userId,tup)
      })

      // 将原始日志数据与黑名单RDD进行join，此时需要使用leftOuterJoin
      // 如果原始日志userId没有在对应的黑名单，一定是join不到的
      val joinedRDD = mappedRDD.leftOuterJoin(blackListRDD)

      // 过滤黑名单
      val filteredRDD = joinedRDD.filter(tup=>{
        // 获取join过来的黑名单的userId对应的Bool值
        val optional = tup._2._2
        // 如果这个值存在，说明原始日志中userId join到了某个黑名单用户，就过滤掉
        if(optional.nonEmpty&&optional.get){
          false
        }else{
          true
        }
      })
      // 返回根据黑名单过滤后的数据
      val resultRDD = filteredRDD.map(tup=>tup._2._1)
      resultRDD
    })
    filteredAdRealTimeLogDStream
  }
  /**
    * 获取黑名单，并转换为RDD
    * @param sc
    * @return
    */
  def getBlackListToRDD(sc: SparkContext) = {
    val adBlacklistDAO = DAOFactory.getAdBlacklistDAO
    val adBlacklists = adBlacklistDAO.findAll

    // 封装黑名单用户，格式为：<userId, true>
    val tuples = new util.ArrayList[(Long, Boolean)]
    for (adBlacklist <- adBlacklists) {
      tuples.add((adBlacklist.getUserid, true))
    }

    // 把获取的黑名单信息生成RDD
    val blackListRDD = sc.parallelize(tuples)

    blackListRDD
  }


  /**
    * 动态生成黑名单
    * @param message
    */
  def dynamicGenerateBlackList(message: InputDStream[(String, String)]): Unit = {
    // 把原始日志数据格式更改为：<yyyyMMdd_userId_adId, 1L>
    val dailyUserAdClickDstream = message.map(tup=>{
      // 从tup中获取每一条原始实时日志
      val log = tup._2
      val logSplited = log.split(" ")

      // 获取yyyyMMdd、userId、adId
      val timestamp = logSplited(0).toLong
      val date = new Date(timestamp)
      val dateKey = DateUtils.formatDate(date)
      val userId = logSplited(3).toLong
      val adId = logSplited(4).toLong

      // 拼接
      val key = dateKey+"_"+userId+"_"+adId
      (key,1L)
    })

    // 针对调整后的日志格式，进行聚合，这样就可以得到每个batch中每天每个用户对每个广告的点击量
    val dailyUserAdClickCountDStream = dailyUserAdClickDstream.reduceByKey(_+_)

    // dailyUserAdClickCountDStream： <yyyyMMdd_userId_adId, count>
    // 把每天各个用户对每个广告的点击量存入数据库
    dailyUserAdClickCountDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(it=>{
        // 对每个分区的数据获取一次数据库连接
        // 每次都是从连接池中获取，而不是每次都创建
        val adUserClickCounts = new util.ArrayList[AdUserClickCount]()
        while (it.hasNext){
          val tuple = it.next
          val keySplited = tuple._1.split("_")

          // 获取String类型日期：yyyy-MM-dd
          val date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited(0)))
          val userId = keySplited(1).toLong
          val adId = keySplited(2).toLong
          val clickCount = tuple._2


          val adUserClickCount = new AdUserClickCount()
          adUserClickCount.setDate(date)
          adUserClickCount.setUserid(userId)
          adUserClickCount.setAdid(adId)
          adUserClickCount.setClickCount(clickCount)
          adUserClickCounts.add(adUserClickCount)
        }

        val adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO
        adUserClickCountDAO.updateBatch(adUserClickCounts)
      })
    })
    // 到这里，在数据中，已经有了累计的每天各用户对各广告的点击量
    // 接下来遍历每个batch中所有的记录
    // 对每天记录都去查询一下这一天每个用户对每个广告的累计点击量
    // 判断，如果某个用户某天对某个广告的点击量大于等于100次
    // 就断定这个用户是一个很黑很黑的黑名单用户，把该用户更新到ad_blacklist表中
    val blackListDStream = dailyUserAdClickCountDStream.filter(tup=>{
      // 切分key得到date、userId、adId
      val keySplited = tup._1.split("_")
      val date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited(0)))

      // yyyy-MM-dd
      val userId = keySplited(1).toLong
      val adId = keySplited(2).toLong

      // 获取点击量
      val adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO
      val clickCount = adUserClickCountDAO.findClickCountByMultiKey(date,userId,adId)

      // 判断是否>=100
      if (clickCount >= 100){
        true
      }else{
        false
      }
    })
    // blackListDStream里面的每个batch，过滤出来的已经在某天某个广告的点击量超过100次的用户
    // 遍历这个DStream的每个RDD， 然后将黑名单用户更新到数据库
    // 注意：blackListDStream中可能有重复的userId，需要去重
    // 首先获取userId
    val blackListUserIdDStream = blackListDStream.map(tup=>{
      val keySplited = tup._1.split("_")
      val userId = keySplited(1).toLong
      userId
    })

    // 根据userId进行去重
    val distinctBlackUserIdDStream = blackListUserIdDStream.transform(rdd=>rdd.distinct)
    // 将黑名单用户进行持久化
    distinctBlackUserIdDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(it=>{
        // 用来存储黑名单用户
        val adBlacklists = new util.ArrayList[AdBlacklist]
        while(it.hasNext){
          val userId = it.next
          val adBlacklist = new AdBlacklist
          adBlacklist.setUserid(userId)
          adBlacklists.add(adBlacklist)
        }
      })
    })

  }


  /**
    * 计算每天各省各城市各广告的点击流量实时统计
    *
    * @param filteredAdRealTimeLogDStream
    * @return
  */
  def calcuateRealTimeStat(filteredAdRealTimeLogDStream: DStream[(String, String)]) = {
    /**
      * 计算该业务，会实时的把结果更新到数据库中
      * J2EE平台就会把结果实时的以各种效果展示出来
      * J2EE平台会每隔几分钟从数据库中获取一次最新的数据
      */
    // 对原始数据进行map，把结果映射为：<date_province_city_adId, 1L>
    val mappedDStream  = filteredAdRealTimeLogDStream.map(tup=>{
      // 把原始数据进行切分并且获取各字段
      val logSplited = tup._2.split(" ")
      val timestamp = logSplited(0)
      val date = new Date(timestamp.toLong)
      val dateKey = DateUtils.formatDateKey(date)

      // yyyyMMdd
      val province = logSplited(1)
      val city = logSplited(2)
      val adId = logSplited(4)
      val key = dateKey+"_"+province+"_"+city+"_"+adId

      (key,1L)
    })

    // 聚合，按批次累加
    // 在这个DStream中，相当于每天各省各城市各广告的点击次数
    val upstateFun = (values: Seq[Long], state: Option[Long]) => {
      // state存储的是历史批次结果
      // 首先根据state来判断，之前的这个key值是否有对应的值
      var clickCount = 0L

      // 如果之前存在值，就以之前的状态作为起点，进行值的累加
      if (state.nonEmpty) clickCount = state.getOrElse(0L)

      // values代表当前batch RDD中每个key对应的所有的值
      // 比如当前batch RDD中点击量为4，values=(1,1,1,1)
      for (value <- values) {
        clickCount += value
      }

      Option(clickCount)
    }

    val aggrDStream = mappedDStream.updateStateByKey(upstateFun)

    // 将结果持久化
    aggrDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(it=>{
        val adStats = new util.ArrayList[AdStat]()
        while(it.hasNext){
          val tuple = it.next()
          val keySplited = tuple._1.split("_")

          val date = keySplited(0)
          val provice = keySplited(1)
          val city = keySplited(2)
          val adId = keySplited(3).toLong
          val clickCount = tuple._2.toLong

          val adStat = new AdStat
          adStat.setDate(date)
          adStat.setProvince(provice)
          adStat.setCity(city)
          adStat.setAdid(adId)
          adStat.setClickCount(clickCount)

          adStats.add(adStat)
        }

        val adStatDAO = DAOFactory.getAdStatDAO
        adStatDAO.updateBatch(adStats)
      })
    })

    aggrDStream
  }

  /**
    * 计每天各省top3热门广告
    *
    * @param adRealTimeStatDStream
    */
  def calculateProvinceTop3Ad(adRealTimeStatDStream: DStream[(String, Long)]): Unit = ???



}
