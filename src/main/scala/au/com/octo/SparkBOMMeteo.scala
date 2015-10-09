package au.com.octo

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import utils.MeteoUtils


object SparkBOMMeteo {

  private val config =  ConfigFactory.load()

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkMeteo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val confFiles = config.getConfig("files")
    val confBom = confFiles.getConfig("bom")

    val fileBom = confBom.getString("fileStations")
    val fileData = confBom.getString("fileData")


    val bomStationsDF: DataFrame = getStationDataframe(sqlContext, fileBom)
    val weatherData: DataFrame = getWeatherDataframe(sqlContext, fileData)

    val closestStations = SparkMeteo.getClosestStations(5, -33.6468, 115.9153, bomStationsDF)
    //val result = getDataForStations("PRCP", "20140507", closestStations, weatherData)
    val result = SparkMeteo.getAllDaysDataForStations("PRCP", closestStations, weatherData)
    closestStations.foreach(println)
    result.foreach(t => {
      println(t._1+" "+t._2)
    })

    System.exit(0)
  }

  def getWeatherDataframe(sqlContext: SQLContext, file:String): DataFrame = {

    val schema = StructType(SparkMeteo.dataHeader.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val weatherRaw = sqlContext.sparkContext.textFile(file)
      .map(l => l.split(",", -1))
      .map(spl => Row(spl(1), spl(2)+spl(3)+spl(4), "PRCP", spl(5)))
    val df = sqlContext.createDataFrame(weatherRaw, schema)
    df.withColumn("v", MeteoUtils.toDouble(df("v")))
  }

  def getStationDataframe(sqlContext: SQLContext, file:String): DataFrame = {

    val schema = StructType(SparkMeteo.stationsHeader.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val stationRaw = sqlContext.sparkContext.textFile(file)
      .map(l => SparkMeteo.trimHeaderRow(getHeaderRow(l)))
    val df = sqlContext.createDataFrame(stationRaw, schema)
    df.withColumn("lat", MeteoUtils.toDouble(df("lat")))
      .withColumn("long", MeteoUtils.toDouble(df("long")))
  }

  def getHeaderRow(l: String): Row = {
    Row(l.substring(0, 8), l.substring(48, 58), l.substring(58, 68))
  }

}



