# Author: 
from datetime import datetime
from pyspark.sql import functions as F
from validate.validation_config import ValidationConfig
from validate.validation_predicates import ValidationPredicates

class ErrorLabeler:
    @staticmethod
    def apply(df):
        c = F.col

        pollutant_errs = [
            F.when(~(ValidationPredicates.nz_num(p) & (c(p) >= 0)), f"{p.upper()}_INVALID")
            for p in ValidationConfig.POLLUTANT_COLS
        ]

        errors_raw = F.array(
            F.when(~(c("city").isNotNull() & (F.length(F.trim("city")) > 0)), "CITY_EMPTY"),
            F.when(~(c("country").isNotNull() & (F.length(F.trim("country")) > 0)), "COUNTRY_EMPTY"),
            F.when(~(ValidationPredicates.nz_num("temp_c")
                     & ValidationPredicates.temp_ok(c("country"), c("temp_c"))),
                   "TEMP_OUT_OF_RANGE_OR_UNKNOWN_COUNTRY"),
            F.when(~(ValidationPredicates.nz_num("humidity_pct") & c("humidity_pct").between(0.0, 100.0)),
                   "HUMIDITY_INVALID"),
            F.when(~(ValidationPredicates.nz_num("wind_ms") & (c("wind_ms") >= 0.0)), "WIND_INVALID"),
            *pollutant_errs,
            F.when(~ValidationPredicates.aqi_ok(c("AQI")), "AQI_INVALID"),
            F.when(c("_corrupt").isNotNull() & (c("_corrupt") != ""), "CSV_CORRUPT"),
        )

        errors = F.filter(errors_raw, lambda x: x.isNotNull())

        return (df
            .withColumn("error_codes", errors)
            .withColumn("error_code", F.element_at("error_codes", 1)) 
            .withColumn(
                "error_message",
                F.concat_ws(" | ",
                    F.lit("failed validation"),
                    F.concat(F.lit("country="), c("country")),
                    F.concat(F.lit("city="),    c("city")),
                    F.concat(F.lit("temp_c="),  c("temp_c").cast("string")),
                    F.concat(F.lit("humidity="),c("humidity_pct").cast("string")),
                    F.concat(F.lit("wind_ms="), c("wind_ms").cast("string")),
                    F.concat(F.lit("AQI="),     c("AQI").cast("string")),
                    F.concat(F.lit("pollutants="),
                             F.concat_ws(",", *[c(p).cast("string") for p in ValidationConfig.POLLUTANT_COLS])),
                )
            )
            .withColumn("ingest_ts", F.lit(datetime.utcnow().isoformat()))
        )
