package day17

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Day 17 — Window Functions
 *
 * Covers:
 *   1. row_number, rank and dense_rank
 *   2. Partitioning by department / customer / route
 *   3. Latest record per customer
 *   4. lag and lead
 *   5. Scenario: top 3 students per course, and latest policy per customer
 *
 * Run with:
 *   sbt "run local[2]"
 *   sbt "run local[4]"
 */
object WindowFunctionsApp {

  def main(args: Array[String]): Unit = {
    val masterUrl = if (args.nonEmpty) args(0) else "local[*]"

    val spark = SparkSession.builder()
      .appName("Day17-WindowFunctionsApp")
      .master(masterUrl)
      .getOrCreate()

    val employeesDf = csv(spark, "employees.csv")
    val policiesDf = csv(spark, "policies.csv")
    val routesDf = csv(spark, "routes.csv")
    val studentsDf = csv(spark, "students.csv")

    section("1 & 2. row_number, rank, dense_rank - partitioned by department")
    rankingDemo(employeesDf)

    section("3. Latest record per customer")
    latestRecordDemo(policiesDf)

    section("4. lag and lead - partitioned by route")
    lagLeadDemo(routesDf)

    section("5. Scenario: top 3 students per course, latest policy per customer")
    scenarioDemo(studentsDf, policiesDf)

    spark.stop()
  }

  def csv(spark: SparkSession, name: String): DataFrame =
    spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"src/main/resources/$name")

  // ---------------------------------------------------------------------
  // 1 & 2. row_number vs rank vs dense_rank, partitioned by department
  // ---------------------------------------------------------------------
  def rankingDemo(employeesDf: DataFrame): Unit = {
    // A window spec is PARTITION BY + ORDER BY (+ an optional frame).
    // Partitioning by department means each department is ranked on its
    // own - the numbering restarts at 1 for every new partition.
    val byDeptSalaryDesc = Window.partitionBy("department").orderBy(col("baseSalary").desc)

    val ranked = employeesDf
      .withColumn("rowNumber", row_number().over(byDeptSalaryDesc))
      .withColumn("rank", rank().over(byDeptSalaryDesc))
      .withColumn("denseRank", dense_rank().over(byDeptSalaryDesc))
      .orderBy("department", "rowNumber")

    println("Engineering only, to see how ties are handled (two employees tied at 95000):")
    ranked.filter(col("department") === "Engineering")
      .select("name", "department", "baseSalary", "rowNumber", "rank", "denseRank")
      .show(truncate = false)

    println(
      """The three differ ONLY on ties:
        | - row_number(): always 1, 2, 3, 4, ... - every row gets a UNIQUE number,
        |   ties broken arbitrarily (by whatever order ties appear in).
        | - rank(): ties share the SAME rank, and the next rank SKIPS ahead by
        |   the number of tied rows (e.g. two rows tied at rank 2 -> next row is rank 4).
        | - dense_rank(): ties share the SAME rank too, but the next rank does NOT
        |   skip - it's always previous rank + 1.
        |""".stripMargin)
  }

  // ---------------------------------------------------------------------
  // 3. Latest record per customer
  // ---------------------------------------------------------------------
  def latestRecordDemo(policiesDf: DataFrame): Unit = {
    val latestWindow = Window.partitionBy("customerId").orderBy(col("effectiveDate").desc)

    val withRowNum = policiesDf.withColumn("rn", row_number().over(latestWindow))

    println("All policies for CUST1, numbered newest-first:")
    withRowNum.filter(col("customerId") === "CUST1")
      .select("policyId", "customerId", "policyType", "effectiveDate", "status", "rn")
      .orderBy("rn")
      .show(truncate = false)

    val latestOnly = withRowNum.filter(col("rn") === 1).drop("rn")
    println(s"Latest policy per customer: ${latestOnly.count()} rows (one per distinct customer)")
    latestOnly.select("customerId", "policyType", "effectiveDate", "status")
      .orderBy("customerId").show(10, truncate = false)

    println(
      """Pattern: partition by the entity (customerId), order by the
        |recency column descending, number the rows, then keep rn == 1.
        |row_number() is the right choice here (not rank/dense_rank) because
        |we want EXACTLY ONE row per customer even if two policies happen
        |to share the same effectiveDate.
        |""".stripMargin)
  }

  // ---------------------------------------------------------------------
  // 4. lag and lead, partitioned by route
  // ---------------------------------------------------------------------
  def lagLeadDemo(routesDf: DataFrame): Unit = {
    val byRouteOrdered = Window.partitionBy("routeId").orderBy("stopOrder")

    val withLagLead = routesDf
      .withColumn("prevStop", lag("stopName", 1).over(byRouteOrdered))
      .withColumn("prevArrival", lag("arrivalTime", 1).over(byRouteOrdered))
      .withColumn("nextStop", lead("stopName", 1).over(byRouteOrdered))
      .withColumn(
        "minutesSincePrevStop",
        (unix_timestamp(col("arrivalTime")) - unix_timestamp(col("prevArrival"))) / 60
      )

    println("Route R1, stop by stop, with the previous/next stop pulled in via lag/lead:")
    withLagLead.filter(col("routeId") === "R1")
      .select("routeId", "stopOrder", "stopName", "arrivalTime", "prevStop", "nextStop", "minutesSincePrevStop")
      .orderBy("stopOrder")
      .show(truncate = false)

    println(
      """lag(col, n)  looks BACKWARD n rows within the partition's order (the
        |             previous stop's value) - null on the first row of each partition.
        |lead(col, n) looks FORWARD n rows (the next stop's value) - null on the
        |             last row of each partition.
        |Both stay WITHIN their own partition - a route never "sees" another
        |route's stops, no matter how the DataFrame's rows are physically ordered.
        |This is exactly how you compute a gap/delta between consecutive rows
        |(time between stops, day-over-day change, previous order's amount, etc.)
        |without a self-join.
        |""".stripMargin)
  }

  // ---------------------------------------------------------------------
  // 5. Scenario: top 3 students per course, latest policy per customer
  // ---------------------------------------------------------------------
  def scenarioDemo(studentsDf: DataFrame, policiesDf: DataFrame): Unit = {
    println("--- Top 3 students per course ---")
    val byCourseScoreDesc = Window.partitionBy("course").orderBy(col("score").desc)

    val rankedStudents = studentsDf
      .withColumn("rankInCourse", dense_rank().over(byCourseScoreDesc))

    // dense_rank is the right tool here: "top 3" should mean the top THREE
    // SCORES, and if two students tie for 3rd place, both of them belong
    // in the top 3 - row_number would arbitrarily cut one of them out.
    val top3 = rankedStudents.filter(col("rankInCourse") <= 3)
      .select("course", "name", "score", "rankInCourse")
      .orderBy(col("course"), col("rankInCourse"), col("score").desc)

    println("(Mathematics has a 3-way tie at 97 - watch how many rows appear for rank 1)")
    top3.show(50, truncate = false)

    println("--- Latest policy per customer (reused from section 3) ---")
    val latestWindow = Window.partitionBy("customerId").orderBy(col("effectiveDate").desc)
    val latestPolicy = policiesDf
      .withColumn("rn", row_number().over(latestWindow))
      .filter(col("rn") === 1)
      .drop("rn")
      .orderBy("customerId")

    latestPolicy.select("customerId", "policyType", "effectiveDate", "status").show(25, truncate = false)

    val activeLatest = latestPolicy.filter(col("status") === "ACTIVE").count()
    val total = latestPolicy.count()
    println(s"\nOf $total customers, $activeLatest have their MOST RECENT policy currently ACTIVE.")
  }

  def section(title: String): Unit = {
    println("\n" + "=" * 70)
    println(title)
    println("=" * 70)
  }
}
