import cms.{read_from_mysql, spark, write_to_mysql}
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Substring

import java.sql.{Connection, DriverManager}
import java.sql.{Connection, DriverManager, Statement}
import java.sql.DriverManager

object auto_attend {

  import sys.process._

  val spark = SparkSession.builder().appName("SparkExample").master("local[*]").getOrCreate()

  import org.apache.spark.sql.{DataFrame, SaveMode}

  def alter_mysql(_df: DataFrame, tableName: String): Unit = {

    val mysqlProperties = new java.util.Properties
    mysqlProperties.setProperty("user", "root")
    mysqlProperties.setProperty("password", "HUKUM@123")
    mysqlProperties.setProperty("driver", "com.mysql.jdbc.Driver")

    val jdbcUrl = "jdbc:mysql://localhost/cms"

    val tabledf = spark.read.jdbc(jdbcUrl, tableName, mysqlProperties)

    val joinedDF = tabledf.join(_df, _df("REMOVE") === tabledf("STUDENT_ID"), "inner")
      .drop("REMOVE").orderBy("STUDENT_ID")

    val dfcopy = joinedDF.persist()
    dfcopy.show()

    joinedDF.write.mode(SaveMode.Overwrite)
      .jdbc(jdbcUrl, tableName, mysqlProperties)

  }

  

  def update_attendance() = {
    val currentDate = spark.sql("SELECT current_date() as current_date").collect()(0)(0)
    val currentDay = spark.sql("SELECT date_format(current_date(), 'EEEE') as current_day").collect()(0)(0)

    val timetable = read_from_mysql(spark, "TIMETABLE")

    run_python()

    val filePath = "C:/Users/Venu Pulagam/Desktop/" + currentDate + "_" + currentDay + ".csv"
    val attendance = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    val classes = timetable.filter(col("DAY") === currentDay)
    val c1 = classes.collect()(0)(1)
    val c2 = classes.collect()(0)(2)
    val c3 = classes.collect()(0)(3)

    val x = attendance.select("Roll No", "SLOT1").withColumnRenamed("Roll No", "REMOVE")
      .withColumnRenamed("SLOT1", currentDate.toString)
    val y = attendance.select("Roll No", "SLOT2").withColumnRenamed("Roll No", "REMOVE")
      .withColumnRenamed("SLOT2", currentDate.toString)
    val z = attendance.select("Roll No", "SLOT3").withColumnRenamed("Roll No", "REMOVE")
      .withColumnRenamed("SLOT3", currentDate.toString)

    //x.show()
    alter_mysql(x, c1.toString)
    alter_mysql(y, c2.toString)
    alter_mysql(z, c3.toString)
  }

  def run_python () = {
    val pythonScriptPath = "C://Users//Venu Pulagam//Desktop//automated_attendance.py"

    // Run the Python script using the "python" command
    val exitCode = Seq("python", pythonScriptPath).!

    // Check the exit code
    if (exitCode == 0) {
      println("Python script executed successfully.")
    } else {
      println(s"Error: Python script execution failed with exit code $exitCode.")
    }
  }


  def main(args: Array[String]): Unit = {
    update_attendance()
  }
}
