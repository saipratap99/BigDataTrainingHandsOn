import org.apache.spark.{SparkConf, SparkContext}

object Main {

  // Spark understands only one data structure - RDD
  // Resilient Distributed Data - RDD
  // RDD -
  // Lazy Loaded of Spark [or] Laxy loading of RDD
  // DAG - Directed Acyclic Graph

  // All class. Instantiate the class in an object
  def main(args : Array[String]) = {
    //   local
    //    local[100]
    //    local[*]
    //    spark
    //    yarn
    //    mesos
    //    kubernetes


    // 1. Spark Config
    val sparkConfig = new SparkConf()
    sparkConfig.setMaster("local[*]")
    sparkConfig.setAppName("AuthenticationDataJob")
    sparkConfig.set("spark.ui.enabled", "true")

    val sparkContext = new SparkContext(sparkConfig);

    val textFileRDD = sparkContext.textFile("hdfs://localhost:9000/auth/auth.csv")

    textFileRDD.union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .foreach(println)



    sparkContext.stop()

  }
}

//case class AuthDTO(aua : String, asa : String)

//    val textFileRDD = sparkContext.textFile("/home/hadoop/Downloads/auth.csv")

// splits each line by , and each col maps to RDD
//    val mappedRDD = textFileRDD.flatMap(each => {
//      each.split(",")
//    })

//    mappedRDD.foreach(each => {
//      println(each)
//    })
//    val returnDT =textFileRDD.count()


//val veggiesRDD = sparkContext.parallelize(Seq("Tomato,apple,Potato",
//  "kiwi,Spinach,Carrot",
//  "Potato,papaya,Garlic"))

