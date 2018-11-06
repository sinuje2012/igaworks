import breeze.linalg._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
import sqlContext.implicits._


// 01-1. EventData.csv 읽기

val event_df = sqlContext.read.format("csv").option("header", "false").load("s3://test-igaworks/EventData.csv")
event_df.printSchema

// 01-2. 속성명 입력

val newnames = Seq("identity_adid", "os", "model", "country", "event_name", "log_id", "server_datetime", "quantity", "price")
val event_df2 = event_df.toDF(newnames: _*)
event_df2.printSchema

// 01-3 임시 뷰테이블 생성
spark.catalog.dropTempView("maindata")
event_df2.createTempView("maindata")

// 01-4 속성타입 변경
var tmp = spark.sql("select identity_adid, os, model, country, event_name, log_id, " +
"cast(server_datetime as timestamp), cast(quantity as int), cast(price as decimal) from maindata")
tmp.printSchema

// 01-5 parquet 으로 저장
// 분할 O
tmp.write.option("header", true).parquet("s3://test-igaworks/EventData_parquet2.parquet")

// 02-1 AttributionData.csv 읽기
var att_df = sqlContext.read.option("header", false).csv("s3://test-igaworks/AttributionData.csv")
att_df.printSchema

// 02-2 컬럼명 입력
var att_df2 = att_df.withColumnRenamed("_c0", "partner").
withColumnRenamed("_c1", "campaign").
withColumnRenamed("_c2", "server_datetime").
withColumnRenamed("_c3", "tracker_id").
withColumnRenamed("_c4", "log_id").
withColumnRenamed("_c5", "attribution_type").
withColumnRenamed("_c6", "identity_adid")
att_df2.printSchema

// 02-3 속성타입 변경
var att_df3 = att_df2.selectExpr("partner", "campaign", "cast(server_datetime as timestamp) server_datetime",
"tracker_id", "log_id", "cast(attribution_type as int) attribution", "identity_adid")
att_df3.printSchema

// 02-4 parquet 으로 저장
// 분할 X
att_df3.repartition(1).write.format("parquet").option("header", true).mode("append").save("s3://test-igaworks/AttributionData_parquet2.parquet")