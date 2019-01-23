package spark.product

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}

/**
  * 组内拼接去重函数
  */
class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction{

  // 指定输入数据的字段与类型
  override def inputSchema: StructType = DataTypes.createStructType(
    util.Arrays.asList(
      DataTypes.createStructField("cityInfo",DataTypes.StringType,true)
    )
  )

  // 指定缓冲数据的字段与类型
  override def bufferSchema: StructType = DataTypes.createStructType(util.Arrays.asList(
    DataTypes.createStructField("bufferCityInfo", DataTypes.StringType, true)
    )
  )

  // 指定返回类型
  override def dataType: DataType =  DataTypes.StringType

  // 指定是否是确定性的
  override def deterministic: Boolean = true


  /**
    * 初始化
    * 可以认为是指定一个初始值
    *
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer.update(0,"")

  /**
    * 局部聚合更新
    * 一个一个的将组内的字段值传递进去
    *
    * @param buffer
    * @param input
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit ={
    // 缓冲中的已经拼接好的城市信息
    var bufferCityInfo = buffer.getString(0)

    // 刚刚传递进来的某个城市信息
    val cityInfo= input.getString(0)
    // 在这里要实现去重的逻辑
    // 需要判断之前没有拼接过某个城市信息，那么才可以拼接
    // 城市信息：0：北京， 1：上海
    if (!bufferCityInfo.contains(cityInfo)) {
      if ("" == bufferCityInfo) {
        bufferCityInfo += cityInfo
      } else {
        bufferCityInfo += "," + cityInfo
      }
    }

    buffer.update(0,bufferCityInfo)

  }


  /**
    * 全局聚合合并
    * update操作：可能是对一个分组内的部分的数据，在某个节点内发生的
    * 但是可能一个分组内的数据会分布在多个节点上处理
    * 此时就要用merge操作，将各个节点上分布式拼接好的字符串合并起来
    *
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var bufferCityInfo1 = buffer1.getString(0)
    val bufferCityInfo2 = buffer2.getString(0)
    for (cityInfo <- bufferCityInfo2.split(",")) {
      if (!bufferCityInfo1.contains(cityInfo)) {
        if ("" == bufferCityInfo1) {
          bufferCityInfo1 += cityInfo
        }
        else {
          bufferCityInfo1 += "," + cityInfo
        }
      }
    }
    buffer1.update(0,bufferCityInfo1)
  }

  /**
    * 作为返回
    *
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = buffer.getString(0)
}
