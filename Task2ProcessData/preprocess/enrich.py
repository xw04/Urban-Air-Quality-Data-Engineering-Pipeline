# Author: 
from pyspark.sql import functions as F

class Enricher:
    @staticmethod
    def apply(df, *, buckets: bool = False, who_flags: bool = False):

        df = df.withColumn("event_date", F.coalesce(F.col("event_date"), F.to_date("ts")))
        df = (
            df
            .withColumn("year",  F.year("event_date"))
            .withColumn("month", F.month("event_date"))
            .withColumn("day",   F.dayofmonth("event_date"))
            .withColumn("day_of_week", F.date_format("event_date", "E"))
            .withColumn("hemisphere",
                F.when(F.col("city").isin("Sydney", "São Paulo"), "S").otherwise("N"))
            .withColumn(
                "season",
                F.when(F.col("hemisphere") == "N",
                      F.when(F.month("event_date").isin(12,1,2), "Winter")
                       .when(F.month("event_date").isin(3,4,5),  "Spring")
                       .when(F.month("event_date").isin(6,7,8),  "Summer")
                       .otherwise("Autumn"))
                 .otherwise(
                      F.when(F.month("event_date").isin(12,1,2), "Summer")
                       .when(F.month("event_date").isin(3,4,5),  "Autumn")
                       .when(F.month("event_date").isin(6,7,8),  "Winter")
                       .otherwise("Spring"))
            )
            .withColumn(
                "pm_ratio",
                F.when(
                    (F.col("pm10_ugm3") > 0) & F.col("pm25_ugm3").isNotNull(),
                    F.col("pm25_ugm3") / F.col("pm10_ugm3")
                )
            )
            .withColumn(
                "aqi_category",
                F.when(F.col("AQI").isNull(), None)
                 .when(F.col("AQI") <= 50,  "Good")
                 .when(F.col("AQI") <= 100, "Moderate")
                 .when(F.col("AQI") <= 150, "Poor")
                 .when(F.col("AQI") <= 200, "Unhealthy")
                 .when(F.col("AQI") <= 300, "Severe")
                 .otherwise("Hazardous")
            )
        )

        if buckets:
            df = (
                df
                .withColumn("aqi_bucket_pm25",
                    F.when(F.col("pm25_ugm3").isNull(), None)
                     .when(F.col("pm25_ugm3") <= 30, "Good")
                     .when(F.col("pm25_ugm3") <= 60, "Moderate")
                     .when(F.col("pm25_ugm3") <= 90, "Poor")
                     .when(F.col("pm25_ugm3") <= 120, "Unhealthy")
                     .when(F.col("pm25_ugm3") <= 250, "Severe")
                     .otherwise("Hazardous"))
                .withColumn("aqi_bucket_pm10",
                    F.when(F.col("pm10_ugm3").isNull(), None)
                     .when(F.col("pm10_ugm3") <= 50,  "Good")
                     .when(F.col("pm10_ugm3") <= 100, "Moderate")
                     .when(F.col("pm10_ugm3") <= 250, "Poor")
                     .when(F.col("pm10_ugm3") <= 350, "Unhealthy")
                     .when(F.col("pm10_ugm3") <= 430, "Severe")
                     .otherwise("Hazardous"))
                .withColumn("aqi_bucket_no2",
                    F.when(F.col("no2_ppb").isNull(), None)
                     .when(F.col("no2_ppb") <= 40,  "Good")
                     .when(F.col("no2_ppb") <= 80,  "Moderate")
                     .when(F.col("no2_ppb") <= 180, "Poor")
                     .when(F.col("no2_ppb") <= 190, "Unhealthy")
                     .when(F.col("no2_ppb") <= 400, "Severe")
                     .otherwise("Hazardous"))
                .withColumn("aqi_bucket_so2",
                    F.when(F.col("so2_ppb").isNull(), None)
                     .when(F.col("so2_ppb") <= 40, "Good")
                     .when(F.col("so2_ppb") <= 80, "Moderate")
                     .when(F.col("so2_ppb") <= 380, "Poor")
                     .when(F.col("so2_ppb") <= 800, "Unhealthy")
                     .when(F.col("so2_ppb") <= 1600, "Severe")
                     .otherwise("Hazardous"))
                .withColumn("aqi_bucket_co",
                    F.when(F.col("co_ppm").isNull(), None)
                     .when(F.col("co_ppm") <= 4.4,  "Good")
                     .when(F.col("co_ppm") <= 9.4,  "Moderate")
                     .when(F.col("co_ppm") <= 12.4, "Poor")
                     .when(F.col("co_ppm") <= 15.4, "Unhealthy")
                     .when(F.col("co_ppm") <= 30.4, "Severe")
                     .otherwise("Hazardous"))
                .withColumn("aqi_bucket_o3",
                    F.when(F.col("o3_ppb").isNull(), None)
                     .when(F.col("o3_ppb") <= 54,  "Good")
                     .when(F.col("o3_ppb") <= 70,  "Moderate")
                     .when(F.col("o3_ppb") <= 85,  "Poor")
                     .when(F.col("o3_ppb") <= 105, "Unhealthy")
                     .when(F.col("o3_ppb") <= 200, "Severe")
                     .otherwise("Hazardous"))
            )

        if who_flags:
            df = (
                df
                .withColumn("pm25_exceeds_who", F.when(F.col("pm25_ugm3") > 15, "Yes").otherwise("No"))
                .withColumn("pm10_exceeds_who", F.when(F.col("pm10_ugm3") > 45, "Yes").otherwise("No"))
                .withColumn("no2_exceeds_who",  F.when(F.col("no2_ppb")   > 21, "Yes").otherwise("No"))
                .withColumn("so2_exceeds_who",  F.when(F.col("so2_ppb")   > 19, "Yes").otherwise("No"))
                .withColumn("co_exceeds_who",   F.when(F.col("co_ppm")    > 4,  "Yes").otherwise("No"))
                .withColumn("o3_exceeds_who",   F.when(F.col("o3_ppb")    > 51, "Yes").otherwise("No"))
                .withColumn(
                    "who_compliant_all",
                    F.when(
                        (F.col("pm25_ugm3") <= 15) &
                        (F.col("pm10_ugm3") <= 45) &
                        (F.col("no2_ppb")   <= 21) &
                        (F.col("so2_ppb")   <= 19) &
                        (F.col("co_ppm")    <= 4)  &
                        (F.col("o3_ppb")    <= 51),
                        "Yes"
                    ).otherwise("No")
                )
            )

        return df
