package au.com.octo

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import utils.MeteoUtils


object SparkGHCNDMeteo {

  // a header has to be added to the files
  // id,date,type,v1,v2,v3,v4,v5
  // the files are on ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/
  val file2012 = "/home/issam/meteo/ghcnd_weather/2012.csv"
  val file2013 = "/home/issam/meteo/ghcnd_weather/2013.csv"
  val file2014 = "/home/issam/meteo/ghcnd_weather/2014.csv"
  val file2015 = "/home/issam/meteo/ghcnd_weather/2015.csv"

  // the original file (found on the ftp)
  // need to be trimmed and a header has to be added
  // ids,lat,long
  val fileGhcnd = "/home/issam/meteo/ghcnd_weather/ghcnd-stations.txt"

	def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkMeteo").setMaster("local[*]")
		val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val ghcndDF: DataFrame = getStationDataframe(sqlContext, fileGhcnd)
    val weatherData: DataFrame = getWeatherDataframe(sqlContext, file2013)


    val closestStations = SparkMeteo.getClosestStations(5, -33.923221, 151.173230, ghcndDF)
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
      .map(spl => Row(spl(0), spl(1), spl(2), spl(3)))
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
    Row(l.substring(0, 11), l.substring(11, 21), l.substring(21, 31))
  }
}



