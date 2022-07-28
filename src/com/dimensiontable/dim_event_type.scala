package com.dimensiontable

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.SaveMode

object dim_event_type extends App {

  Logger.getLogger("org").setLevel(Level.ERROR);

  val spark = SparkSession.builder().master("local[*]")
    .appName("populatingDimEventType")
    .getOrCreate()

  val bucket = "edtech_analytics_dump"
  spark.conf.set("temporaryGcsBucket", bucket)

  val campaign_details_Schema = StructType(List(
    StructField("campaign_id", IntegerType),
    StructField("course_campaign_name", StringType),
    StructField("user_id", IntegerType),
    StructField("user_name", StringType),
    StructField("campaign_date", DateType),
    StructField("digital_marketing_team", StringType),
    StructField("event_status", IntegerType),
    StructField("event_type", StringType),
    StructField("marketing_product", StringType),
    StructField("user_response_time", IntegerType)));
  val df1 = (spark.read.format("csv")
    .schema(campaign_details_Schema)
    .option("header", true)
    .option("path", "gs://edtech_analytics_dump/data/bq-results.csv")
    .load())

  df1.createOrReplaceTempView("userEventDetails");

  /**
   * Populating dim event type details
   */
  
    val transformedDf = spark.sql("""select row_number() over(order by event_type) as event_key, event_type, 
     case when event_Type='email_open' then 'All email open events are captured here' 
     when event_type='email_no_open' then 'All email no open events are captured here'
    when event_type='email_click' then 'All email click events are captured here'
    when event_type='email_no_click' then 'All email no click events are captured here'
    when event_type='email_bounce' then 'All email bounce events are captured here'
    when event_type='email_sent' then 'All email sent events are captured here'
    when event_type='email_unsubscribe' then 'All email no subscribe events are captured here'
    when event_type='email_no_unsubscribe' then 'All email subscribe events are captured here'
    else 'No Event' end as event_Description from userEventDetails group by event_type order by event_type""")

    (transformedDf.write.format("bigquery")
      .option("table", "dimension_tables.dim_event_type")
      .mode(SaveMode.Append)
      .save())

  

}