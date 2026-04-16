# Author: 

from pyspark.sql import functions as F
from validate.validation_config import ValidationConfig

class ValidationPredicates:
    @staticmethod
    def nz_num(colname: str):
        c = F.col(colname)
        return c.isNotNull() & ~F.isnan(c)

    @staticmethod
    def temp_ok(country_col, temp_col):
        expr = None
        for ctry, (lo, hi) in ValidationConfig.TEMP_BOUNDS_BY_COUNTRY.items():
            cond = (country_col == F.lit(ctry)) & temp_col.between(float(lo), float(hi))
            expr = cond if expr is None else (expr | cond)
        return expr if expr is not None else F.lit(False)

    @staticmethod
    def aqi_ok(col):
        return col.isNotNull() & ~F.isnan(col) & col.between(
            ValidationConfig.AQI_MIN, ValidationConfig.AQI_MAX
        )
