package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{stddev_pop, stddev_samp}
import org.apache.spark.sql.Row
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SQLContext}
import java.sql.{Connection, DriverManager, ResultSet, Timestamp}

import org.apache.spark.rdd.RDD

import scala.collection._
import scala.collection.mutable

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo = pickupInfo.filter(pickupInfo("_c0") =!= "vendor_name")
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  println(pickupInfo.count())
  // kabbom filter
  pickupInfo = pickupInfo.filter(pickupInfo("x") >= minX and pickupInfo("x") <= maxX and pickupInfo("y") >= minY and pickupInfo("y") <= maxY and pickupInfo("z") >= minZ and pickupInfo("z") <= maxZ)
  println(pickupInfo.count())

  //groupby
  pickupInfo = pickupInfo.groupBy("x", "y", "z").count()
  val mean = pickupInfo.agg(avg(pickupInfo("count"))).first().get(0).asInstanceOf[Double].doubleValue()
  val sd = pickupInfo.agg(stddev(pickupInfo("count"))).first().get(0).asInstanceOf[Double].doubleValue()
  val list = pickupInfo.collectAsList()
  var mapped = new mutable.HashMap[String, Integer]()
  for(_row <- list.toArray()) {
    val row = _row.asInstanceOf[Row]
    val x= row.get(0).asInstanceOf[Integer].intValue()
    val y = row.get(1).asInstanceOf[Integer].intValue()
    val z= row.get(2).asInstanceOf[Integer].intValue()
    val count= row.get(3).asInstanceOf[Long].intValue()
    mapped.put(""+x+","+y+","+z, count)
  }
  import spark.sqlContext.implicits._

  var final_df3 = pickupInfo.map(_row=>{
    val row = _row.asInstanceOf[Row]
    val x= row.get(0).asInstanceOf[Integer].intValue()
    val y = row.get(1).asInstanceOf[Integer].intValue()
    val z= row.get(2).asInstanceOf[Integer].intValue()
    var sum = 0
    var neighbours= 0
    for (ix <- Seq(-1, 0 ,1)){
      for (iy <- Seq(-1, 0 ,1)) {
        for (iz <- Seq(-1, 0 ,1)) {
          val nx=ix+x
          val ny=iy+y
          val nz =iz+z
          val key = ""+nx+","+ny+","+nz
          var filtered = mapped.get(key)
          if(filtered.orNull!=null){
            val count = filtered.orNull.intValue()
            sum += count
            neighbours+=1
          }
        }
      }
    }

    val z_score = (sum - (mean * neighbours))/ (sd * Math.sqrt((numCells* neighbours - neighbours*neighbours)/ (numCells-1)))
    if(x==null || y==null || z==null){
      println("Nullllllllllll")
    }
    (x, y, z, z_score)//""+x+","+y+","+z+","+z_score
  }).toDF(Seq("x", "y", "z", "zz"):_*)
    final_df3 = final_df3.orderBy(final_df3("zz").desc).drop("zz")
/*
  //zscore
  val list_string_output = mutable.MutableList[(Int, Int, Int, Double)]()
  for(_row <- list.toArray()){
    val row = _row.asInstanceOf[Row]
    val x= row.get(0).asInstanceOf[Integer].intValue()
    val y = row.get(1).asInstanceOf[Integer].intValue()
    val z= row.get(2).asInstanceOf[Integer].intValue()
    var sum = 0
    var neighbours= 0
    for (ix <- Seq(-1, 0 ,1)){
      for (iy <- Seq(-1, 0 ,1)) {
        for (iz <- Seq(-1, 0 ,1)) {
          val nx=ix+x
          val ny=iy+y
          val nz =iz+z
          val key = ""+nx+","+ny+","+nz
          var filtered = mapped.get(key)
          if(filtered.orNull!=null){
            val count = filtered.orNull.intValue()
            sum += count
            neighbours+=1
          }
        }
      }
    }

    val z_score = (sum - (mean * neighbours))/ (sd * Math.sqrt((numCells* neighbours - neighbours*neighbours)/ (numCells-1)))
    if(x==null || y==null || z==null){
      println("Nullllllllllll")
    }
    val str= (x, y, z, z_score)//""+x+","+y+","+z+","+z_score
    list_string_output+=str
  }
  import spark.sqlContext.implicits._
  var final_df = list_string_output.toDF(Seq("x", "y", "z", "_4"):_*).orderBy("_4")

  final_df = final_df.drop("_4")
*/
  /*val rows = list_string_output.map{x => Row(x:_*)}
  val rdd = spark.sparkContext.makeRDD[RDD](rows)
  val df = spark.sqlContext.createDataFrame(rdd, schema)

  pickupInfo = pickupInfo.drop("count")
  val exprs = pickupInfo.schema.fields.map { f =>
    if (final_df.schema.fields.contains(f)) col(f.name)
    else lit(null).cast(f.dataType).alias(f.name)
  }
*/
  //final_df.select(exprs: _*).printSchema

  return final_df3 // YOU NEED TO CHANGE THIS PART
}
}
