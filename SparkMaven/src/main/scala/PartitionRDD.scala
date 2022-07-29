import org.apache.spark.Partitioner
import org.apache.spark.sql.SparkSession

object PartitionRDD {
  def main(args: Array[String]): Unit ={
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("PartitionJob")
      .getOrCreate()

    val sparkContext = sparkSession.sparkContext;

    val textFileRDD = sparkContext.textFile("hdfs://localhost:9000/auth/auth.csv");

    val statePairRDD = textFileRDD.map(row => row.split(","))
      .map(cols => (cols(128), cols(128)));

    statePairRDD.partitionBy(new StatePartitioner).saveAsTextFile("hdfs://localhost:9000/output/")

  }
}


class StatePartitioner extends Partitioner{
  override def numPartitions: Int = 50

  override def getPartition(key: Any): Int = {
    val state = key.asInstanceOf[String];
    if(state.equalsIgnoreCase("Uttar Pradesh"))
      return 32;
    else if (state.equalsIgnoreCase("Tamil Nadu"))
      return 2;
    else
      return 28;
  }
}
