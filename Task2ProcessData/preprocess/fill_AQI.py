# Author: 
from pyspark.sql import functions as F

class AQI:
    _BPTS = {
        "pm25_ugm3":[(0.0,12.0,0,50),(12.1,35.4,51,100),(35.5,55.4,101,150),
                     (55.5,150.4,151,200),(150.5,250.4,201,300),(250.5,350.4,301,400),
                     (350.5,500.4,401,500)],
        "pm10_ugm3":[(0.0,54.0,0,50),(55.0,154.0,51,100),(155.0,254.0,101,150),
                     (255.0,354.0,151,200),(355.0,424.0,201,300),(425.0,504.0,301,400),
                     (505.0,604.0,401,500)],
        "o3_ppb":[(0.0,54.0,0,50),(55.0,70.0,51,100),(71.0,85.0,101,150),
                  (86.0,105.0,151,200),(106.0,200.0,201,300),(201.0,604.0,301,500)],
        "co_ppm":[(0.0,4.4,0,50),(4.5,9.4,51,100),(9.5,12.4,101,150),
                  (12.5,15.4,151,200),(15.5,30.4,201,300),(30.5,40.4,301,400),
                  (40.5,50.4,401,500)],
        "no2_ppb":[(0.0,53.0,0,50),(54.0,100.0,51,100),(101.0,360.0,101,150),
                   (361.0,649.0,151,200),(650.0,1249.0,201,300),(1250.0,1649.0,301,400),
                   (1650.0,2049.0,401,500)],
        "so2_ppb":[(0.0,35.0,0,50),(36.0,75.0,51,100),(76.0,185.0,101,150),
                   (186.0,304.0,151,200),(305.0,604.0,201,300),(605.0,804.0,301,400),
                   (805.0,1004.0,401,500)],
    }

    @staticmethod
    def _segment(col, segs):
        c = F.col(col)
        expr=None
        for (cl,ch,Il,Ih) in reversed(segs):
            seg = (c>=F.lit(cl)) & (c<=F.lit(ch))
            val = ((F.lit(Ih-Il)/F.lit(ch-cl))*(c-F.lit(cl))+F.lit(Il))
            expr = F.when(seg,val) if expr is None else F.when(seg,val).otherwise(expr)
        lowest=segs[0]; highest=segs[-1]
        expr = F.when(c< F.lit(lowest[0]), F.lit(lowest[2])) \
                .otherwise(F.when(c>F.lit(highest[1]), F.lit(highest[3])).otherwise(expr))
        return F.when(c.isNull(), F.lit(None)).otherwise(expr)

    @staticmethod
    def impute_aqi(df):
        subs=[]
        for col,segs in AQI._BPTS.items():
            if col in df.columns:
                df = df.withColumn(f"sub_{col}", AQI._segment(col, segs))
                subs.append(F.col(f"sub_{col}"))
        if subs:
            df = df.withColumn("AQI", F.coalesce(F.col("AQI"), F.greatest(*subs)))
            for col in list(AQI._BPTS.keys()):
                helper = f"sub_{col}"
                if helper in df.columns: df = df.drop(helper)
        return df