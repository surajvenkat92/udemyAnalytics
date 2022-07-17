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

object dim_campaign extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR);
  
   val spark = SparkSession.builder().master("local[*]")
    .appName("populatingDimCampaign")
    .getOrCreate()
    
      
    val bucket = "edtech_analytics_dump"
  spark.conf.set("temporaryGcsBucket", bucket)

  val campaign_details_Schema = StructType(List(
    StructField("campaign_id", IntegerType),
    StructField("course_campaign_name", StringType),
    StructField("campaign_agenda", StringType),
    StructField("campaign_category", StringType),
    StructField("campaign_agenda_sent", IntegerType),
    StructField("campaign_agenda_open", IntegerType),
    StructField("campaign_agenda_click", IntegerType),
    StructField("campaign_agenda_unsubscribe", IntegerType),
    StructField("digital_marketing_team", StringType),
    StructField("course_campaign_start_date", DateType),
    StructField("course_campaign_end_date", DateType),
    StructField("marketing_product", StringType)));
   
   
   val df1 = (spark.read.format("bigquery")
      .schema(campaign_details_Schema)
    .option("header", true)
    .option("path", "gs://edtech_analytics_dump/data/campaign_details/part-00000-c1d16a54-9a4f-4f4d-9401-60a6b1ef7983-c000.csv")
    .load())
    
    val dimCampaign = (spark.read.format("bigquery")
                           .option("table", "dimension_tables.dim_campaign")
                            .load())
    
    df1.createOrReplaceTempView("campaign_details");
   dimCampaign.createOrReplaceTempView("dim_campaign")
   
   val dimCampaign_count=dimCampaign.count()
   
   if(dimCampaign_count >0){
        val transformedDf =  spark.sql("""select 
                            (select max(campaign_key) from dim_campaign)+row_number() over(order by campaign_id) as campaign_key
                            ,campaign_id,course_campaign_start_date as course_campaign_start_date, date_add(course_campaign_start_date,14) as course_campaign_end_date,
                            campaign_agenda,campaign_category,campaign_agenda_sent,campaign_agenda_open,campaign_agenda_click,campaign_agenda_unsubscribe from campaign_details""")
  
        (transformedDf.write.format("bigquery")
      .option("table", "dimension_tables.dim_campaign")
      .mode(SaveMode.Append)
      .save())
     
   }else{
         val transformedDf =  spark.sql("""select row_number() over(order by campaign_id) as campaign_key,campaign_id,course_campaign_start_date as course_campaign_start_date, date_add(course_campaign_start_date,14) as course_campaign_end_date,
                        campaign_agenda,campaign_category,campaign_agenda_sent,campaign_agenda_open,campaign_agenda_click,campaign_agenda_unsubscribe from campaign_details""")
  
         (transformedDf.write.format("bigquery")
      .option("table", "dimension_tables.dim_campaign")
      .mode(SaveMode.Append)
      .save())               
                        
   }
                        
                        
                        
  
}