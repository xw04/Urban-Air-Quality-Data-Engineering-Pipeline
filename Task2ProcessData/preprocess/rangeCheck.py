#rangeCheck.py
from pyspark.sql import functions as F
class RangeGuards:

    TEMP_BOUNDS_BY_COUNTRY = {
        "USA": (-85.0, 60.0),
        "France": (-45.0, 50.0),
        "India": (-55.0, 55.0),
        "Brazil": (-15.0, 45.0),
        "Egypt": (5.0, 55.0),
    }
    GLOBAL_TEMP_MIN = -90.0
    GLOBAL_TEMP_MAX =  60.0

    @staticmethod
    def _temp_min_map(F_):
        kv = []
        for k, (mn, _mx) in RangeGuards.TEMP_BOUNDS_BY_COUNTRY.items():
            kv += [F_.lit(k), F_.lit(mn)]
        return F_.create_map(*kv) if kv else F_.create_map()

    @staticmethod
    def _temp_max_map(F_):
        kv = []
        for k, (_mn, mx) in RangeGuards.TEMP_BOUNDS_BY_COUNTRY.items():
            kv += [F_.lit(k), F_.lit(mx)]
        return F_.create_map(*kv) if kv else F_.create_map()

    @staticmethod
    def column_ranges(df, mode: str = "nullify"):
        F_ = F
        tmin_by = RangeGuards._temp_min_map(F_)
        tmax_by = RangeGuards._temp_max_map(F_)

        tmin = F_.coalesce(F_.element_at(tmin_by, F_.col("country")),
                           F_.lit(RangeGuards.GLOBAL_TEMP_MIN))
        tmax = F_.coalesce(F_.element_at(tmax_by, F_.col("country")),
                           F_.lit(RangeGuards.GLOBAL_TEMP_MAX))

        if "temp_c" in df.columns:
            temp = F_.col("temp_c")
            temp_oob = temp.isNotNull() & ((temp < tmin) | (temp > tmax))
            repaired_temp = (
                F_.when(temp_oob, F_.least(F_.greatest(temp, tmin), tmax)) if mode == "clip"
                else F_.when(temp_oob, F_.lit(None))
            ).otherwise(temp)
            df = df.withColumn("temp_c", repaired_temp)

        if "humidity_pct" in df.columns:
            hum = F_.col("humidity_pct")
            hum_bad = hum.isNotNull() & ((hum < 0) | (hum > 100))
            repaired_hum = (
                F_.when(hum_bad, F_.least(F_.greatest(hum, F_.lit(0.0)), F_.lit(100.0))) if mode == "clip"
                else F_.when(hum_bad, F_.lit(None))
            ).otherwise(hum)
            df = df.withColumn("humidity_pct", repaired_hum)

        nonneg_candidates = ["pm25_ugm3","pm10_ugm3","no2_ppb","so2_ppb","co_ppm","o3_ppb","AQI","wind_ms"]
        nonneg = [c for c in nonneg_candidates if c in df.columns]
        for c in nonneg:
            colc = F_.col(c)
            neg = colc.isNotNull() & (colc < 0)
            fixed = (F_.when(neg, F_.lit(0.0)) if mode == "clip" else F_.when(neg, F_.lit(None))).otherwise(colc)
            df = df.withColumn(c, fixed)

        return df

