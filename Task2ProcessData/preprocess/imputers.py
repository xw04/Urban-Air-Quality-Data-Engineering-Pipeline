# Author: 
from pyspark.sql import functions as F

class Imputers:
    @staticmethod
    def numeric(df):
        num_cols = [c for c in [
            "pm25_ugm3","pm10_ugm3","no2_ppb","so2_ppb","co_ppm","o3_ppb",
            "temp_c","humidity_pct","wind_ms"
        ] if c in df.columns]
        if not num_cols:
            return df

        gvals = {}
        for c in num_cols:
            try:
                q = df.approxQuantile(c, [0.5], 0.05)
                gvals[c] = q[0] if q else None
            except Exception:
                gvals[c] = None

        aggs_country = [F.expr(f"percentile_approx({c}, 0.5, 50)").alias(c) for c in num_cols]
        ctr_country = df.groupBy("country").agg(*aggs_country).collect()

        aggs_city = [F.expr(f"percentile_approx({c}, 0.5, 50)").alias(c) for c in num_cols]
        ctr_city = df.groupBy("country", "city").agg(*aggs_city).collect()

        def map_from_pairs(pairs):
            if not pairs:
                return F.create_map()
            keys = F.array(*[F.lit(k) for k,_ in pairs])
            vals = F.array(*[F.lit(v) for _,v in pairs])
            return F.map_from_arrays(keys, vals)

        city_median_map = {
            c: map_from_pairs([
                (row["country"] + "|" + row["city"], row[c])
                for row in ctr_city
                if row["country"] is not None and row["city"] is not None and row[c] is not None
            ])
            for c in num_cols
        }

        country_median_map = {
            c: map_from_pairs([
                (row["country"], row[c])
                for row in ctr_country
                if row["country"] is not None and row[c] is not None
            ])
            for c in num_cols
        }

        out = df
        for c in num_cols:
            out = out.withColumn(
                c,
                F.coalesce(
                    F.col(c),
                    F.element_at(city_median_map[c], F.concat_ws("|", F.col("country"), F.col("city"))),
                    F.element_at(country_median_map[c], F.col("country")),
                    F.lit(gvals[c])
                )
            )
        return out