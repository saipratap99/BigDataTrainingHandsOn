import org.apache.spark.sql.SparkSession

object EthereumTransactionsJob {
  def main(args: Array[String]): Unit ={
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("EthereumTransactionsJob")
      .getOrCreate();

    val sparkContext = sparkSession.sparkContext;

    val textFileRDD = sparkContext.textFile("hdfs://localhost:9000/ethereum/txn_et.csv");

    val txnPairRDD = textFileRDD.map(txn => txn.split(","))
      .filter(txn => !txn(8).equals("value"))
      .map(txn => (txn(6), BigInt(txn(8))))

    val maxTxn = txnPairRDD.aggregateByKey(BigInt(0))((_+_),(_+_))
      .max()(new Ordering[(String, BigInt)] {
        override def compare(x: (String, BigInt), y: (String, BigInt)): Int = {
          Ordering[BigInt].compare(x._2,y._2);
        }
      });

    println(s"Top spent address ${maxTxn._1} of ${maxTxn._2} Eth")

  }
}
