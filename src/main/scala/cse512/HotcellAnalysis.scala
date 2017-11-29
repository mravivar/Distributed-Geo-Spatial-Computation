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
  var mean=0.0
  var sd=0.0
  var numCells=0.0
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val NEIGHBORS_ON_CORNERS = 7
  val NEIGHBORS_ON_EDGE = 11
  val NEIGHBORS_ON_FACE = 17
  val NEIGHBORS_ON_INSIDE = 26
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
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*).persist()

  // Define the min and max of x, y, z

  numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  println(pickupInfo.count())
  // kabbom filter
  pickupInfo = pickupInfo.filter(pickupInfo("x") >= minX and pickupInfo("x") <= maxX and pickupInfo("y") >= minY and pickupInfo("y") <= maxY and pickupInfo("z") >= minZ and pickupInfo("z") <= maxZ)
    .orderBy("x", "y", "z")
  println(pickupInfo.count())
  //groupby
  pickupInfo=pickupInfo.groupBy("x", "y", "z").count().persist()
  mean = pickupInfo.agg(avg(pickupInfo("count"))).first().get(0).asInstanceOf[Double].doubleValue()
  sd = pickupInfo.agg(stddev(pickupInfo("count"))).first().get(0).asInstanceOf[Double].doubleValue()

  pickupInfo.createOrReplaceTempView("pickupinfo")

  var joinedResult = spark.sql("select t1.x as t1x,t1.y as t1y,t1.z as t1z,t2.x as t2x,t2.y as t2y,t2.z as t2z,t2.count as t2count from pickupinfo as t1 cross join pickupinfo as t2 " +
    "where (t1.x==t2.x or t1.x==t2.x-1 or t1.x==t2.x+1) and (t1.y==t2.y or t1.y==t2.y-1 or t1.y==t2.y+1) and (t1.z==t2.z or t1.z==t2.z-1 or t1.z==t2.z+1)"
    )

  joinedResult = joinedResult.groupBy(joinedResult("t1x"), joinedResult("t1y"), joinedResult("t1z")).agg(sum(joinedResult("t2count")))
  joinedResult = joinedResult.withColumnRenamed("sum(t2count)", "weight")

  spark.udf.register("countNeighbours", countNeighbours _)
  joinedResult.createOrReplaceTempView("withoutNeighbours")
  joinedResult = spark.sql("select t1x,t1y,t1z,weight,countNeighbours(t1x,t1y,t1z) as neighbours from withoutNeighbours")
  joinedResult.createOrReplaceTempView("joinedResult")
  spark.udf.register("zscore",(neighbours:Int, weight:Int)=>(zscore(neighbours, weight)))
  var final_res = spark.sql("select t1x as x, t1y as y, t1z as z, zscore(neighbours, weight) as z_score from joinedResult").orderBy(desc("z_score"))
  final_res.show(50)
  final_res = final_res.drop("z_score")
  /*

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
  return final_res // YOU NEED TO CHANGE THIS PART
}
  def zscore(neighbours: Int, sum:Int): Double ={
    val nr = sum- (mean* neighbours)
    val dr = sd * Math.sqrt((numCells* neighbours - neighbours*neighbours)/ (numCells-1))
    return nr/dr
  }

  def countNeighbours(x:Int, y:Int, z:Int): Int={
    var terminalIndex=0
    if(x==minX || x==maxX){
      terminalIndex+=1
    }
    if(y==minY || y==maxY){
      terminalIndex+=1
    }
    if(z==minZ|| z==maxZ){
      terminalIndex+=1
    }
    var neighbours=0
    if(terminalIndex==1){
      neighbours=NEIGHBORS_ON_FACE
    }else if(terminalIndex==2){
      neighbours=NEIGHBORS_ON_EDGE
    }else if(terminalIndex==3){
      neighbours=NEIGHBORS_ON_CORNERS
    }else{
      neighbours=NEIGHBORS_ON_INSIDE
    }
    return neighbours
  }
}
