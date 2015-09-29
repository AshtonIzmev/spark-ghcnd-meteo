package test.scala.au.com.octo

import au.com.octo.SparkMeteo
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}


@RunWith(classOf[JUnitRunner])
class SparkMeteoTest extends FlatSpec with Matchers {

  val stationTestDf: DataFrame = TestUtils.createStationsTestDF()
  val weatherTestDf: DataFrame = TestUtils.createWeatherGHCNDTestDF()

  "the closest 2 stations" should "computed with euclidian distance" in {

    // test
    val resul = SparkMeteo.getClosestStations(1, 24.3390, 57.5970, stationTestDf)

    // assert
    resul.size should be (1)
    resul.head._1 should be ("AE000041196")
  }

  "The PRCP value" should "have the right value or a particular date" in {
    // given
    val map = Map("USC00178997" -> 1.0, "USC00178998" -> 2.0, "US1TXTV0133" -> 0.0 )

    // test
    val resul = SparkMeteo.getDataForStations("PRCP", "20140101", map, weatherTestDf)

    // assert
    resul should equal (3.58 +- 0.01)
  }

  "Data for one day" should "be correct" in {
    // given
    val map = Map("USC00178997" -> 1.0, "USC00178998" -> 2.0, "US1TXTV0133" -> 0.0 )

    // test
    val resul = SparkMeteo.getAllDaysDataForStations("PRCP", map, weatherTestDf)

    // assert
    resul.size should be (2)
    resul(0)._1 should equal ("20140102")
    resul(0)._2 should equal (14.58 +- 0.01)
    resul(1)._1 should equal ("20140101")
    resul(1)._2 should equal (3.58 +- 0.01)
  }

}

object TestUtils {

  val sparkConfig = new SparkConf()
  val sc = new SparkContext("local", "test", sparkConfig)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  private val nullable: Boolean = true

  def createStationsTestDF() :DataFrame = {
    val schema =
      StructType(
        Array(
          StructField("id", StringType, nullable),
          StructField("lat", DoubleType, nullable),
          StructField("long", DoubleType, nullable)
        )
      )
    val rowRDD = sc.parallelize(Array(
      Row("ACW00011604", 17.1167, -61.7833),
      Row("ACW00011647", 17.1333, -61.7833),
      Row("AE000041196", 25.3330, 55.5170),
      Row("AEM00041194", 25.2550, 55.3640),
      Row("AEM00041217", 24.4330, 54.6510),
      Row("AF000040930", 35.3170, 69.0170)))
    sqlContext.createDataFrame(rowRDD, schema)
  }

  def createWeatherGHCNDTestDF() :DataFrame = {
    val schema =
      StructType(
        Array(
          StructField("id", StringType, nullable),
          StructField("date", StringType, nullable),
          StructField("type", StringType, nullable),
          StructField("v", DoubleType, nullable)
        )
      )
    val rowRDD = sc.parallelize(Array(
      Row("US1FLSL0019", "20140101", "PRCP", 0.0),
      Row("US1TXTV0134", "20140101", "PRCP", 22.0),
      Row("US1TXTV0133", "20140101", "PRCP", Double.NaN),
      Row("USC00178998", "20140101", "PRCP", -22.1),
      Row("USC00178997", "20140101", "PRCP", 15.0),
      Row("USC00178998", "20140102", "PRCP", -20.1),
      Row("USC00178997", "20140102", "PRCP", 30.0)
      )
    )
    sqlContext.createDataFrame(rowRDD, schema)
  }
}