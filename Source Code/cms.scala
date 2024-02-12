import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Substring
import org.apache.spark.sql.functions.{col, current_date, datediff}
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.expressions.Window
import org.sparkproject.dmg.pmml.False

object cms{

  // FUNCTION TO WRITE A DATAFRAME IN SPARK AS A TABLE IN MYSQL

  def write_to_mysql(_df: DataFrame, tableName: String): Unit = {
    _df.write.format("jdbc")
      .option("url", "jdbc:mysql://localhost/cms")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", tableName)
      .option("user", "root")
      .option("password", "HUKUM@123")
      .mode("Append")
      .save()
  }

  // FUNCTION TO WRITE A DATAFRAME IN SPARK FROM A TABLE IN MYSQL

  def read_from_mysql(_spark: SparkSession, tableName: String): DataFrame = {
    _spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost/cms")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", tableName)
      .option("user", "root")
      .option("password", "HUKUM@123")
      .load()
  }

  val spark = SparkSession.builder().appName("SparkExample").master("local[*]").getOrCreate()

  // ANALYSIS 1 : BEST REASON TO GET A PASS

  def bestreason() = {
    val passtable = "PASSES"
    val passDF = read_from_mysql(spark, passtable)
    val outDF = passDF.where(col("APPROVAL_STATUS") === "Approved").groupBy("REASON")
      .agg(count("*").alias("PROBABILITY"))
    val tot = outDF.agg(sum("PROBABILITY").alias("TOTAL")).collect()(0)(0)
    val newDF = outDF.withColumn("PROBABILITY", round(col("PROBABILITY") / tot, 3))
      .orderBy(desc("PROBABILITY"))
    newDF.show()
    val maxprob = newDF.collect()(0)(1)
    val bestreason = newDF.collect()(0)(0)
    print("With the highest probability " + maxprob + ", " + bestreason + " is the best reason to get a Pass issued")
  }

  // ANALYSIS 2 : DEPARTMENT WISE RANKING OF DRUG CONSUMPTION

  def drugcons() = {
    val susptable = "SUSPENSION"
    val suspdf = read_from_mysql(spark, susptable)
    val counctable = "COUNCELING_INFO"
    val councdf = read_from_mysql(spark, counctable)
    val resultDF = suspdf.filter(col("Reason").contains("alcohol") ||
      col("Reason").contains("cigarette"))
      .select("STUDENT_ID", "REASON")
    val outDF = resultDF.withColumn("REASON", translate(col("REASON"), "\r", ""))
    val newDF = outDF.join(councdf, outDF("STUDENT_ID") === councdf("STUDENT_ID"), "inner")
    val totDF = newDF.groupBy(col("DEPARTMENT_ADMISSION").alias("DEPARTMENT_ID"))
      .agg(count("REASON").alias("NUMBER OF DRUG CONSUMERS")).orderBy(desc("NUMBER OF DRUG CONSUMERS"))
    totDF.show(Int.MaxValue)
  }

  // ANALYSIS - 3 : MOST WANTED DEPARTMENT

  def mostwanteddept () = {
    val counctable = "COUNCELING_INFO"
    val councdf = read_from_mysql(spark, counctable)
    val deptable = "DEPARTMENTS"
    val depdf = read_from_mysql(spark, deptable)

    val joinedDF = councdf.join(depdf, councdf("DEPARTMENT_CHOICES") === depdf("DEPARTMENT_ID"), "inner")
    val wanted = joinedDF.groupBy("DEPARTMENT_CHOICES")
      .agg(count("STUDENT_ID").alias("COUNT"), first("DEPARTMENT_NAME")
        .alias("DEPARTMENT_NAME"))
      .select("DEPARTMENT_NAME", "COUNT")
    wanted.orderBy(desc("COUNT")).show(5, truncate = false)

  }

  // ANALYSIS 4 : TOPPER FOR EACH SEMESTER

  def topofsem () = {
    val markstable = "MARKS"
    val marksdf = read_from_mysql(spark, markstable)
    val paperstable = "PAPERS"
    val papersdf = read_from_mysql(spark, paperstable)

    val joinedDF = papersdf.join(marksdf, papersdf("PAPER_ID") === marksdf("PAPER_ID"), "inner")
    val resultDF = joinedDF.groupBy("SEMESTER_NAME", "STUDENT_ID")
      .agg(round(sum("MARKS")/7,2).alias("PERCENTAGE"))
    val outDF = resultDF
      .orderBy(desc("PERCENTAGE"))
      .groupBy("SEMESTER_NAME")
      .agg(
        first("STUDENT_ID").alias("TOP_STUDENT_ID"),
        max("PERCENTAGE").alias("TOP_PERCENTAGE")
      )
    outDF.show()
  }

  // ANALYSIS 5 : DEPARTMENTS WITH LEAST NUMBER OF STUDENTS

  def leastdep() = {
    val counctable = "COUNCELING_INFO"
    val councdf = read_from_mysql(spark, counctable)
    val resultDF = councdf.groupBy(col("DEPARTMENT_ADMISSION").alias("DEPARTMENT_ID"))
      .agg(count("STUDENT_ID").alias("NUMBER OF STUDENTS"))
    val outDF = resultDF.orderBy(asc("NUMBER OF STUDENTS"))
    outDF.show(5)
  }

  // ANALYSIS 6 : PROBABILITY OF GETTING A PASS IN CASE OF MEDICAL EMERGENCY

  def medemg () = {
    val passtable = "PASSES"
    val passDF = read_from_mysql(spark, passtable)
    val resultDF = passDF.where("REASON = 'Medical Emergency'")
    val outDF = resultDF.groupBy("APPROVAL_STATUS").agg(count("*").alias("PROBABILITY"))
    val tot = outDF.agg(sum("PROBABILITY").alias("SUM_PROBABILITY")).collect()(0)(0)
    val newDF = outDF.withColumn("PROBABILITY", round(col("PROBABILITY")/tot, 2))
    newDF.show()

  }

  // ANALYSIS 7 : MOST SENIOR FACULTY FROM EACH DEPARTMENT

  def senfac() = {
    val windowSpec = Window.partitionBy("DEPARTMENT_ID").orderBy("DOJ")
    val empstable = "EMPLOYEES"
    val empsdf = read_from_mysql(spark, empstable)
    val resultDF = empsdf
      .withColumn("rank", rank().over(windowSpec))
      .filter(col("rank") === 1)
      .drop("rank")
      .select("DEPARTMENT_ID", "DOJ", "EMPLOYEE_ID")
      .withColumn("DOJ", col("DOJ").cast("date"))
      .withColumn("WORKING_FOR", year(current_date()) - year(col("DOJ")))

    resultDF.show()
  }

  // ANALYSIS 8 : NO. OF STUDENTS NOT HAVING MET THE ATTENDANCE CRITERION

  def attcrit(PaperID: String): Long = {
    val attentable = "ATTENDANCE"
    val attenDF = read_from_mysql(spark, attentable)
    val tottable = "TOTCLASS"
    val totDF = read_from_mysql(spark, tottable)
    val res = totDF.filter(col("PAPER_ID") === PaperID)
    val num_classes = res.collect()(0)(1).asInstanceOf[Int]
    val class_id = res.collect()(0)(2)
    val req_classes = scala.math.ceil(0.75 * num_classes).toLong
    val overDF = attenDF.select("STUDENT_ID", class_id.toString)
    val outDF = overDF.withColumn("PERCENTAGE", round(col(class_id.toString) * 100 / num_classes, 2))
      .filter(col("PERCENTAGE") < 75.00)
    val resDF = outDF.withColumn("MUST_ATTEND", expr(s"$req_classes - ${class_id.toString}"))
      .withColumnRenamed(class_id.toString, "ATTENDED")
    resDF.show()
    print(resDF.count() + " out of " + attenDF.count() + " Students lack Minimum Attendance in " + PaperID)

    return resDF.count()
  }


  import spark.implicits._

  // VISUALISATION : AVG NO. OF ATTENDEES DURING THE ENTIRE SEMESTER

  def subvsatt () = {
    val attenDF = read_from_mysql(spark, "ATTENDANCE")
    val A = attenDF.agg(sum("A")/count("STUDENT_ID")).collect()(0)(0).asInstanceOf[Double]
    val B = attenDF.agg(sum("B")/count("STUDENT_ID")).collect()(0)(0).asInstanceOf[Double]
    val C = attenDF.agg(sum("C")/count("STUDENT_ID")).collect()(0)(0).asInstanceOf[Double]
    val D = attenDF.agg(sum("D")/count("STUDENT_ID")).collect()(0)(0).asInstanceOf[Double]
    val E = attenDF.agg(sum("E")/count("STUDENT_ID")).collect()(0)(0).asInstanceOf[Double]
    val F = attenDF.agg(sum("F")/count("STUDENT_ID")).collect()(0)(0).asInstanceOf[Double]
    val G = attenDF.agg(sum("G")/count("STUDENT_ID")).collect()(0)(0).asInstanceOf[Double]

    val sub = Seq(("SEMI0022256", A), ("SEMI0022443", B), ("SEMI0024747", C),
      ("SEMI0025077", D), ("SEMI0025909", E), ("SEMI0029061", F), ("SEMI0029604", G))
    val rdd = spark.sparkContext.parallelize(sub)
    val df = rdd.toDF("PAPER_ID", "AVG_ATTENDANCE")
    df.show()

    /**val path = "C:/Users/Venu Pulagam/Downloads/subvsatt.csv"
    df.write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv(path)**/
  }

  // VISUALISATION : ADMISSION ANALYSIS ON IDEPT2425 IN THE PERIOD 2013-2018

  def deptwant () = {

    import breeze.plot._
    import breeze.plot.Plot._
    import breeze.linalg._

    val counctable = "COUNCELING_INFO"
    val councdf = read_from_mysql(spark, counctable)

    val resdf = councdf.filter(col("DEPARTMENT_ADMISSION").contains("IDEPT2425"))

    val outDF = resdf
      .groupBy("DOA")
      .agg(count("STUDENT_ID").alias("COUNT"))
      .select(substring(col("DOA"), 1, 4).alias("YEAR"), col("COUNT"))

    outDF.show()
    val newdf = outDF.withColumn("YEAR", col("YEAR").cast("Int")).orderBy("YEAR")
    val years = newdf.collect().map(row => row.getAs[Int]("YEAR"))
    val num = newdf.collect().map(row => row.getAs[Long]("COUNT"))
    val f = Figure()
    val p = f.subplot(0)
    val data = years.zip(num) // Combine x and y data into pairs
    p += plot(data.map(_._1.toDouble), data.map(_._2.toDouble), '+', name = "Scatter Plot")
    p += plot(data.map(_._1.toDouble), data.map(_._2.toDouble), '-', name = "Lines")
    p.xlabel = "YEAR"
    p.ylabel = "NO. OF STUDENTS JOINED IN IDEPT2425"
    f.saveas("deptwant.png")
  }

  // VISUALISATION : DEPT VS NUMBER OF STUDENTS VS NUMBER OF LOW PERFORMERS IN 2014, SEM - 05

  def poorintot () = {
    val councDF = read_from_mysql(spark, "COUNCELING_INFO")
    val marksDF = read_from_mysql(spark, "MARKS")
    val res = marksDF.filter(col("STUDENT_ID").contains("SID2014")).filter(col("PAPER_ID")
      .contains("SEMI005")).groupBy("STUDENT_ID").agg((sum("MARKS")/7).alias("PERCENTAGE"))
    val tojoin = councDF.select("DEPARTMENT_ADMISSION", "STUDENT_ID")
      .withColumnRenamed("STUDENT_ID", "remove")
    val out = res.join(tojoin, tojoin("remove") === res("STUDENT_ID"), "Inner").drop("remove")
    val output = out.filter(col("PERCENTAGE") <= 80.00).groupBy("DEPARTMENT_ADMISSION")
      .agg((count("PERCENTAGE")).alias("COUNT"))
    val tojoin2 = councDF.filter(col("STUDENT_ID").contains("SID2014")).select("DEPARTMENT_ADMISSION", "STUDENT_ID")
      .withColumnRenamed("DEPARTMENT_ADMISSION", "remove").groupBy("remove")
      .agg((count("STUDENT_ID")).alias("TOTAL_STUDENTS"))
    val result = output.join(tojoin2, tojoin2("remove") === output("DEPARTMENT_ADMISSION"), "inner").drop("remove")
    result.show(Int.MaxValue)

    /**val path = "C:/Users/Venu Pulagam/Downloads/poorintot.csv"
    result.write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv(path)**/
  }

  // VISUALISATION : PASS REASONS DISTRIBUTION

  def distreasons () = {
    val passDF = read_from_mysql(spark, "PASSES")
    val out = passDF.groupBy("REASON").agg(count("STUDENT_ID").alias("NO. OF STUDENTS"))
    out.show()

    /** val path = "C:/Users/Venu Pulagam/Downloads/distreasons.csv"
     * out.write
     * .mode(SaveMode.Append)
     * .option("header", "true")
     * .csv(path)* */
  }

  // MAIN FUNCTION TO CALL THE EXISTING FUNCTIONS

  def main(args: Array[String]): Unit = {


    /**bestreason()
    println("")
    drugcons()
    println("")
    mostwanteddept()
    println("")
    topofsem()
    println("")
    leastdep()
    println("")
    medemg()
    println("")
    senfac()
    println("")
    attcrit("SEMI0029604")
    println("")

    deptwant()
    subvsatt()
    poorintot()
    distreasons()**/

    }
}