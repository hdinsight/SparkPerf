package com.databricks.spark.sql.perf.sql.hibench

class HiBenchQueries {

  val queries = Seq(
    ("join",
      """
        |SELECT
        |    sourceIP,
        |    avg(pageRank),
        |    sum(adRevenue) as totalRevenue
        |FROM rankings R JOIN
        |    (SELECT
        |         sourceIP,
        |         destURL,
        |         adRevenue FROM uservisits_copy UV
        |     WHERE (
        |         datediff(UV.visitDate, '1999-01-01') >=0
        |             AND
        |         datediff(UV.visitDate, '2000-01-01')<=0)
        |           ) NUV ON (R.pageURL = NUV.destURL)
        |     GROUP BY sourceIP
        |     ORDER BY totalRevenue DESC
      """.
        stripMargin),
    ("aggregation",
      """
        |SELECT sourceIP, SUM(adRevenue) FROM uservisits GROUP BY sourceIP
      """.stripMargin),
    ()
  )
}
