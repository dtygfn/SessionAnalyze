package spark.session

/**
  * 自定义排序：
  * 进行排序需要几个字段：点击次数、下单次数、支付次数
  * 需要实现Ordered接口中的几个方法进行排序
  * 需要与其它几个key进行比较，判断大于、大于等于、小于、小于等于
  * 必须要实现Serializable接口进行序列化
  * 其中clickCount=点击次数、orderCount=下单次数、payCount=支付次数
  */
class CategorySortKey(var clickCount:Long,var orderCount:Long,var payCount:Long)
    extends Ordered[CategorySortKey] with Serializable {
  override def compare(that: CategorySortKey): Int = {
    if (clickCount - that.clickCount != 0) return (clickCount - that.clickCount).toInt
    else if (orderCount - that.orderCount != 0) return (orderCount - that.orderCount).toInt
    else if (payCount - that.payCount != 0) return (payCount - that.payCount).toInt
    0
  }

}
