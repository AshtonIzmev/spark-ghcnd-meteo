package au.com.octo

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import utils.MeteoUtils


object SparkBOMMeteo {

  // a header has to be added to the files
  // id,date,type,v1,v2,v3,v4,v5
  // the files are on ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/
  val fileData = "/home/issam/meteo/bom_weather/IDCJAC0009_9989_1800_Data.csv"
  val file2013 = "/home/issam/meteo/bom_weather/*.csv"

  // the original file (found on the ftp)
  // need to be trimmed and a header has to be added
  // ids,lat,long
  val fileBom = "/home/issam/meteo/bom_weather/alphaAUS_139.txt"

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkMeteo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val bomStationsDF: DataFrame = getStationDataframe(sqlContext, fileBom)
    val weatherData: DataFrame = getWeatherDataframe(sqlContext, fileData)


    val closestStations = SparkMeteo.getClosestStations(5, -33.923221, 151.173230, bomStationsDF)
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
      .map(l => l.split(","))
      .map(spl => Row(spl(1), spl(2)+spl(3)+spl(4), "PRCP", spl(5)))
    val df = sqlContext.createDataFrame(weatherRaw, schema)
    df.withColumn("v", MeteoUtils.toDouble(df("v")))
  }

  def getStationDataframe(sqlContext: SQLContext, file:String): DataFrame = {

    val schema = StructType(SparkMeteo.stationsHeader.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val stationRaw = sqlContext.sparkContext.textFile(file)
      .map(l => Row(l.substring(0,7), l.substring(50,59), l.substring(59,69)))
    val df = sqlContext.createDataFrame(stationRaw, schema)
    df.withColumn("lat", MeteoUtils.toDouble(df("lat")))
      .withColumn("long", MeteoUtils.toDouble(df("long")))
  }
}



