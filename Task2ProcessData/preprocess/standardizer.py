# Author: 
from pyspark.sql import functions as F, types as T

class Standardizer:
    SCHEMA = T.StructType([
        T.StructField("Date",              T.StringType()),
        T.StructField("City",              T.StringType()),
        T.StructField("Country",           T.StringType()),
        T.StructField("AQI",               T.DoubleType()),
        T.StructField("PM2.5 (µg/m³)",     T.DoubleType()),
        T.StructField("PM10 (µg/m³)",      T.DoubleType()),
        T.StructField("NO2 (ppb)",         T.DoubleType()),
        T.StructField("SO2 (ppb)",         T.DoubleType()),
        T.StructField("CO (ppm)",          T.DoubleType()),
        T.StructField("O3 (ppb)",          T.DoubleType()),
        T.StructField("Temperature (°C)",  T.DoubleType()),
        T.StructField("Humidity (%)",      T.DoubleType()),
        T.StructField("Wind Speed (m/s)",  T.DoubleType()),
        T.StructField("_corrupt",          T.StringType()),
    ])

    @staticmethod
    def _simplify(name: str) -> str:
        if name is None: return ""
        s = name.strip()
        s = (s.replace("µ", "u")
               .replace("μ", "u")
               .replace("³", "3")
               .replace("°", "deg")
               .replace("(", " ").replace(")", " ")
               .replace("/", " ").replace("-", " ").replace("_", " "))
        s = " ".join(s.split()).lower()
        return s

    _CANONICAL_HEADERS = {
        "date": "Date",
        "city": "City",
        "country": "Country",
        "aqi": "AQI",
        "pm2.5 ug m3": "PM2.5 (µg/m³)",
        "pm2 5 ug m3": "PM2.5 (µg/m³)",
        "pm10 ug m3": "PM10 (µg/m³)",
        "no2 ppb": "NO2 (ppb)",
        "so2 ppb": "SO2 (ppb)",
        "co ppm": "CO (ppm)",
        "o3 ppb": "O3 (ppb)",
        "temperature degc": "Temperature (°C)",
        "humidity %": "Humidity (%)",
        "humidity": "Humidity (%)",
        "wind speed m s": "Wind Speed (m/s)",
        "_corrupt": "_corrupt",
        "corrupt record": "_corrupt",
        "columnnameofcorruptrecord": "_corrupt",
    }

    _RENAME_TO_SNAKE = {
        "PM2.5 (µg/m³)": "pm25_ugm3",
        "PM10 (µg/m³)" : "pm10_ugm3",
        "NO2 (ppb)"    : "no2_ppb",
        "SO2 (ppb)"    : "so2_ppb",
        "CO (ppm)"     : "co_ppm",
        "O3 (ppb)"     : "o3_ppb",
        "Temperature (°C)": "temp_c",
        "Humidity (%)"     : "humidity_pct",
        "Wind Speed (m/s)": "wind_ms",
    }

    @staticmethod
    def _apply_header_canonicalization(df):
        if "_corrupt" not in df.columns:
            df = df.withColumn("_corrupt", F.lit(None).cast(T.StringType()))
        current = df.columns
        rename_pairs = []
        for c in current:
            simp = Standardizer._simplify(c)
            target = Standardizer._CANONICAL_HEADERS.get(simp)
            if target and target != c:
                rename_pairs.append((c, target))

        for src, dst in rename_pairs:
            if dst not in df.columns:
                df = df.withColumnRenamed(src, dst)
        return df

    @staticmethod
    def _norm_city_expr(colname: str):
        c = F.regexp_replace(F.trim(F.col(colname)), r"\s+", " ")
        return F.initcap(c)

    @staticmethod
    def _norm_country_expr(colname: str):
        c = F.regexp_replace(F.trim(F.col(colname)), r"\s+", " ")
        return F.when(F.length(c) <= 3, F.upper(c)).otherwise(F.initcap(c))

    @staticmethod
    def _parse_ts(colname: str):
        c = F.col(colname)
        return F.coalesce(
            F.to_timestamp(c, "d/M/yyyy"),
            F.to_timestamp(c, "dd/MM/yyyy"),
            F.to_timestamp(c, "M/d/yyyy"),
            F.to_timestamp(c, "MM/dd/yyyy"),
            F.to_timestamp(c, "yyyy-MM-dd"),
            F.to_timestamp(c) 
        )

    @staticmethod
    def apply(df):
        df = Standardizer._apply_header_canonicalization(df)

        exprs = []
        if "Date" in df.columns:
            df = df.withColumn("ts", Standardizer._parse_ts("Date"))
        if "City" in df.columns:
            df = df.withColumn("city", Standardizer._norm_city_expr("City"))
        if "Country" in df.columns:
            df = df.withColumn("country", Standardizer._norm_country_expr("Country"))

        for src, dst in Standardizer._RENAME_TO_SNAKE.items():
            if src in df.columns and dst not in df.columns:
                df = df.withColumnRenamed(src, dst)

        drop_cols = [c for c in ["Date", "City", "Country"] if c in df.columns]
        if drop_cols:
            df = df.drop(*drop_cols)

        num_cols = ["AQI","pm25_ugm3","pm10_ugm3","no2_ppb","so2_ppb","co_ppm","o3_ppb","temp_c","humidity_pct","wind_ms"]
        for c in num_cols:
            if c in df.columns:
                df = df.withColumn(c, F.col(c).cast(T.DoubleType()))

        front = [c for c in ["ts", "city", "country"] if c in df.columns]
        rest  = [c for c in df.columns if c not in front]
        return df.select(*front, *rest)
