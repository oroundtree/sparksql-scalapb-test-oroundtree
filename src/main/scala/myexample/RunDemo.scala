package myexample

import com.example.protos.demo.{Address, Gender, Person}
import org.apache.spark.sql.SparkSession

object RunDemo {

  def main(Args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ScalaPB Demo").getOrCreate()

    val sc = spark.sparkContext

    import scalapb.spark.Implicits._

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


    val testPersons = Seq(testPerson1, testPerson2, testPerson3)
    val serialized = testPersons.map(_.toByteArray)

    val serializedDf = spark.createDataset(serialized)
    serializedDf.show()
    val personsDf = serializedDf.map(Person.parseFrom)

    personsDf.show()
  }
}
