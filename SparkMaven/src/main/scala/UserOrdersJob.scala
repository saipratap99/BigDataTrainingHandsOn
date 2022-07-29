import org.apache.spark.sql.SparkSession

object UserOrdersJob {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("UserOrdersJob")
      .master("local[*]")
      .getOrCreate();

    val sparkContext = sparkSession.sparkContext;

    val textFileRDD = sparkContext.textFile("/home/hadoop/Downloads/orders.txt")
    val totalSpentAccumulator = sparkContext.doubleAccumulator("TotalSumAccum");
    val userBroadcast = sparkContext.broadcast(User(3, "Sai Pratap"));

    val userOrdersRDD = textFileRDD.map(row => row.split(" "))
      .map(cols => (Order(cols(0).toInt, cols(1).toInt,cols(2).toDouble)))
      .filter(eachOrder =>  userBroadcast.value.userId == eachOrder.userId);

    userOrdersRDD.foreach(order => totalSpentAccumulator.add(order.amount))

    println(s"${userBroadcast.value.name} spent ${totalSpentAccumulator.value} on orders")

  }
}

case class Order(orderId: Int, userId: Int, amount: Double);
case class User(userId: Int, name: String);
