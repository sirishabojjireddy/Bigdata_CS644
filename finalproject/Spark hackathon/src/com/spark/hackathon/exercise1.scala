package com.spark.hackathon
import org.apache.spark.sql.SparkSession
case class Incidents(incidentnum: String, category: String, description: String, dayofweek: String, date: String,
                     time: String, pddistrict: String, resolution: String, address: String, X: Double, Y: Double, pdid: String)
object exercise1 {
  def main(args: Array[String]) {
    println("Welcome to Inceptez Hackathon Exercise 2")

    val spark = SparkSession.builder().appName("Exercise1").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    val sfpd_load = spark.read.format("csv").option("delimiter", ",").option("header", "true").option("inferSchema", "true").load("hdfs://localhost:54310/user/hduser/sparkhack_1/sfpd.csv")
    import spark.implicits._
    val sfpd_DF = sfpd_load.toDF("incidentnum", "category", "description", "dayofweek", "date", "time", "pddistrict", "resolution", "address", "X", "Y", "pdid")
    //*********Dataframe to DataSet Conversion*********************//
    val sfpdDS = sfpd_DF.as[Incidents]

    //********Creation of Table*******************************//

    sfpdDS.createOrReplaceTempView("sfpd")
//****************Top 5 Districts with Incidents*******************************//
    val incByDistSQL = spark.sql("select pddistrict as District, count(incidentnum) as tot_incident from sfpd group by pddistrict order by tot_incident DESC").show(5, false)

//****************Top 10 resolutions  with Incidents*******************************//
    val top10ResSQL = spark.sql(" select resolution , count(incidentnum) as tot_incident from sfpd group by resolution order by tot_incident DESC LIMIT 10").show(false)

//*****************Top 3 Category with Incidents ******************************//
    
    val top3CatSQL = spark.sql("Select category, count(incidentnum) as tot_incident from sfpd group by category order by tot_incident DESC LIMIT 3").show(false)
    
//**************Writing to HDFS Location as JSON format************************//    
  
   // sfpdDS.coalesce(1).write.format("json").save("hdfs://localhost:54310/user/hduser/sfpd_json")
    println("Written to HDFS as JSON Format")
 
//***********Contains Warrant****************************************************//
    
val WarrantsSQL = spark.sql("select * from sfpd")

WarrantsSQL.filter($"category".like("WARRANTS") || $"description".like("warrant") ).show(false)
    
//*********************Spark UDF Function***************************//

spark.udf.register("getyear", (inputdt:String)=>{inputdt.substring(inputdt.lastIndexOf('/')+1) })
//*********************Count of incidents By year*******************///
val incyearSQL = spark.sql("SELECT getyear(date), count(incidentnum) AS countbyyear FROM sfpd GROUP BY getyear(date) ORDER BY countbyyear DESC").show(false)


//*****************Incidents reported in 2014***************************//

val inc2014 = spark.sql("select category, address, resolution from sfpd where getyear(date) = 14").show(false)

//*********************vandalism address and resolutions in 2015**************************//

val van2015 = spark.sql("select address, resolution from sfpd where getyear(date) = 15 and category ='VANDALISM' ").show(false)




  }

}