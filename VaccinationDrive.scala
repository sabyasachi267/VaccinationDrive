import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.mutable.{ Map => MutableMap }
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.DataFrame

/**Creating a spark session for APP_NAME ="VACCINATION_PROGRAM"*/
val spark = SparkSession
  .builder()
  .appName("VACCINATION_PROGRAM")
  .getOrCreate()

/**Reading the sample CSV dataset from local file system e.g. dbfs*/
val sampleEmployee = spark.read.format("csv")
  .option("header", true)
  .load("dbfs:/FileStore/shared_uploads/sahoo.sabyasachi7@gmail.com/us_500.csv")
//employeeRaw.printSchema()
val resultData1 = sampleEmployee.count()
println("count sampleEmployee:" + resultData1)
sampleEmployee.show(20, false)

/**Based on the input dataset generating 100X datapoints in the source dataframe*/
val employeeDF = sampleEmployee.withColumn("dummy", explode(array((1 until 101).map(lit): _*)))
  .selectExpr(sampleEmployee.columns: _*)
val resultData = employeeDF.count()
println("employeeDF with 100X Datapoints:" + resultData)
employeeDF.show(20, false)

/**Grouping on the city to get headcounts per each city and sorting with descending to get city with max headcount*/
val CityEmployeeDensity = employeeDF.groupBy("city").agg(count("*").alias("COUNT_CITY")).orderBy(desc("COUNT_CITY")) //.show(false)
//employeeDF.groupBy("first_name").count().show(false)
val result = CityEmployeeDensity.count()
println("count CityEmployeeDensity:" + CityEmployeeDensity)
CityEmployeeDensity.show(10, false)

/**Creating a  column Sequence and column value is being insered from CityEmployeeDensity dataframe and created one more column STATUS to check the vaccination status*/
val VaccinationDrivePlan = employeeDF.withColumn("Sequence", CityEmployeeDensity("city"))
  .withColumn("Status", lit("N"))
//VaccinationDrivePlan.printSchema()
println("VaccinationDrivePlan:")
VaccinationDrivePlan.show(10, false)

/**Grouping by city and prepairing a list of employees to iterate through the batch of 100 persons each time*/
val vaccinationDF = VaccinationDrivePlan.groupBy("city")
  .agg(collect_list(col("first_name").cast(StringType)).alias("first_name"))

/**Looping through the entire employee/city dataset for a batch of 100 each time for updating the vaccination status*/
var res_string: String = ""
vaccinationDF.filter(col("city").isNotNull).collect().foreach(
  row => {
    val listNumbers = row.getAs[collection.mutable.WrappedArray[String]](1).toArray.toList
    val batch_size = 100
    //println("====listNumbers===")
    //println(listNumbers)
    if (listNumbers.length != 0) {
      var batch_so_num_list = listNumbers.grouped(batch_size)
      for (so_nums <- batch_so_num_list) {
        var vaccination_resp: String = "Y"
        //fsl_resp.withColumn("Status",lit("Y"))
        res_string = vaccination_resp
      }
    }
  })
println("res_string :" + res_string)

/**Creating a dataframe for the responces received for the vaccination for each city of a batch of 100*/
val vaccStatus =
  sc.parallelize(List(res_string)).toDF("Vaccination_status")
println("vaccStatus:")
vaccStatus.show(10, false)

/**Joining vaccStatus with VaccinationDrivePlan to get the final set of vaccinated employees*/
val finalVaccinatedDF = VaccinationDrivePlan.crossJoin(vaccStatus)
  .drop(VaccinationDrivePlan("status"))
println("finalVaccinatedDF:")
finalVaccinatedDF.show(10, false)

/**Final Dataframe shows the count of days per City for total Vaccination*/
val finalDataset = CityEmployeeDensity
  .withColumn("date_forecast", col("COUNT_CITY").divide(100))
println("finalDataset:")
finalDataset.show(10, false)