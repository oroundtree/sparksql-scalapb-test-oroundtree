import com.example.protos.demo._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class SparkSqlScalaPBTests extends AnyFunSuite {

  val sparkConf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("appName")
    .set("spark.ui.showConsoleProgress", "false")

  lazy val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  val testPerson1: Person = Person().update(
    _.name := "Joe",
    _.age := 32,
    _.gender := Gender.MALE)

  val testPerson2: Person = Person().update(
    _.name := "Mark",
    _.age := 21,
    _.gender := Gender.MALE,
    _.addresses := Seq(
      Address(city = Some("San Francisco"), street = Some("3rd Street"))
    ))

  val testPerson3: Person = Person().update(
    _.name := "Steven",
    _.gender := Gender.MALE,
    _.addresses := Seq(
      Address(city = Some("San Francisco"), street = Some("5th Street")),
      Address(city = Some("Sunnyvale"), street = Some("Wolfe"))
    ))


  test("Test serialization and deserialization with implicits") {
    import scalapb.spark.Implicits._

    val testPersons = Seq(testPerson1, testPerson2, testPerson3)
    val serialized = testPersons.map(_.toByteArray)

    val serializedDf = spark.createDataset(serialized)
    serializedDf.show()
    val personsDf = serializedDf.map(Person.parseFrom)

    personsDf.show()
  }


  test("Encode string, spark implicits") {
    import spark.implicits._
    val mySeq = Seq("hello", "world")
    val myDf = spark.createDataset(mySeq)
    myDf.show()
  }

  test("Encode string, scalapb implicits") {
    import scalapb.spark.Implicits._
    val mySeq = Seq("hello", "world")
    val myDf = spark.createDataset(mySeq)
    myDf.show()
  }
}
