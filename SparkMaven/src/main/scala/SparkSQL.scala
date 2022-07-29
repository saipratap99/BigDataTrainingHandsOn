import org.apache.spark.sql.SparkSession;

object SparkSQL {
  def main(args: Array[String]): Unit ={
    val sparkSession = SparkSession.builder()
                                   .master("local[*]").appName("SQLJob").getOrCreate()

    val sparkContext = sparkSession.sparkContext;

    val csvDataFrame = sparkSession.
      read.
      option("header", true)
      .csv("/home/hadoop/Downloads/auth.csv")
      .select("auth_code","sa","asa")


    csvDataFrame.createOrReplaceTempView("AUTH1")
    csvDataFrame.createOrReplaceTempView("AUTH2")

    val dataDF = sparkSession.sql("select a1.*,a2.* from AUTH1 as a1 " +
                                            "LEFT OUTER JOIN AUTH2 as a2 " +
                                            "on a1.auth_code = a2.auth_code")
                .show()



  }
}


