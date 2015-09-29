package au.com.octo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import utils.MeteoUtils


object SparkMeteo {

  // a header has to be added to the files
  // id,date,type,v1,v2,v3,v4,v5
  // the files are on ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/
  val file2012 = "/home/issam/meteo/2012.csv"
  val file2013 = "/home/issam/meteo/2013.csv"
  val file2014 = "/home/issam/meteo/2014.csv"
  val file2015 = "/home/issam/meteo/2015.csv"

  // the original file (found on the ftp)
  // need to be trimmed and a header has to be added
  // ids,lat,long
  val fileGhcnd = "/home/issam/meteo/ghcnd-stations.csv"

	def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkMeteo").setMaster("local[*]")
		val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val ghcndDF: DataFrame = getStationDataframe(sqlContext, fileGhcnd)
    val weatherData: DataFrame = getWeatherDataframe(sqlContext, file2013)


    val closestStations = getClosestStations(5, -33.923221, 151.173230, ghcndDF)
    //val result = getDataForStations("PRCP", "20140507", closestStations, weatherData)
    val result = getAllDaysDataForStations("PRCP", closestStations, weatherData)
    closestStations.foreach(println)
    result.foreach(t => {
      println(t._1+" "+t._2)
    })

		System.exit(0)
	}

  def getWeatherDataframe(sqlContext: SQLContext, file:String): DataFrame = {
    val weatherRaw = MeteoUtils.csvToDF(sqlContext, file)
    weatherRaw.withColumn("v1", MeteoUtils.toDouble(weatherRaw("v1")))
  }

  def getStationDataframe(sqlContext: SQLContext, file:String): DataFrame = {
    val ghcndRaw = MeteoUtils.csvToDF(sqlContext, file)
    ghcndRaw
      .withColumn("lat", MeteoUtils.toDouble(ghcndRaw("lat")))
      .withColumn("long", MeteoUtils.toDouble(ghcndRaw("long")))
  }

  def getClosestStations(nb:Int, lat:Double, long:Double, stations:DataFrame): Map[String, Double] = {
    stations
      .map(r => (Math.pow(r.getDouble(1)-lat, 2) + Math.pow(r.getDouble(2)-long, 2), r.getString(0)))
      .takeOrdered(nb)(Ordering.by[(Double, String), Double](_._1))
      .map(v => v._2 -> v._1).toMap
  }

  def getDataForStations(tp:String, dt:String, stationsFilt:Map[String, Double],
                             weatherData:DataFrame) :Double = {
    // dt format YYYYMMdd
    val tuple = weatherData
      .filter(weatherData("id").isin(stationsFilt.keySet.toList:_*))
      .filter(weatherData("type").equalTo(tp).and(weatherData("date").equalTo(dt)))
      .map(getVal(stationsFilt, _))
      .filter(!_._1.isNaN)
      .map(v => (v._1*v._2, v._2))
      .reduce((u,v) => (u._1+v._1, u._2+v._2))

    return tuple._1/tuple._2
  }

  def getAllDaysDataForStations(tp:String, stationsFilt:Map[String, Double],
                         weatherData:DataFrame) :Array[(String, Double)] = {
    weatherData
      .filter(weatherData("id").isin(stationsFilt.keySet.toList:_*))
      .filter(weatherData("type").equalTo(tp))
      .map(r => (r.getString(1), getVal(stationsFilt, r)))
      .filter(!_._2._1.isNaN)
      .map(v => (v._1, (v._2._1*v._2._2, v._2._2)))
      .reduceByKey((u,v) => (u._1+v._1, u._2+v._2))
      .map(a => (a._1, a._2._1/a._2._2))
      .collect()
  }

  def getVal(stationsFilt: Map[String, Double], r: Row): (Double, Double) = {
    (r.getDouble(3), MeteoUtils.spatialPonderation(stationsFilt.get(r.getString(0)).get))
  }

}



