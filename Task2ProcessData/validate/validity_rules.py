# Author: 
from pyspark.sql import functions as F
from validate.validation_config import ValidationConfig
from validate.validation_predicates import ValidationPredicates

class ValidityRules:
    @staticmethod
    def apply(df):
        c = F.col

        pollutants_ok = F.lit(True)
        for p in ValidationConfig.POLLUTANT_COLS:
            pollutants_ok &= (ValidationPredicates.nz_num(p) & (c(p) >= 0))

        return df.withColumn(
            "is_valid",
            c("city").isNotNull() & (F.length(F.trim("city")) > 0) &
            c("country").isNotNull() & (F.length(F.trim("country")) > 0) &
            pollutants_ok &
            ValidationPredicates.nz_num("temp_c")
            & ValidationPredicates.temp_ok(c("country"), c("temp_c")) &
            ValidationPredicates.nz_num("humidity_pct") & c("humidity_pct").between(0.0, 100.0) &
            ValidationPredicates.nz_num("wind_ms") & (c("wind_ms") >= 0.0) &
            ValidationPredicates.aqi_ok(c("AQI")) &
            (c("_corrupt").isNull() | (c("_corrupt") == ""))
        )
