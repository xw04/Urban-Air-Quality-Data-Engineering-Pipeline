# Author: 

class ValidationConfig:
    TEMP_BOUNDS_BY_COUNTRY = {
        "USA": (-85.0, 60.0), "UK": (-30.0, 45.0), "France": (-45.0, 50.0),
        "China": (-55.0, 55.0), "India": (-55.0, 55.0), "Japan": (-45.0, 45.0),
        "Australia": (-25.0, 55.0), "Brazil": (-15.0, 45.0), "Egypt": (5.0, 55.0),
    }
    AQI_MIN, AQI_MAX = 0.0, 500.0
    POLLUTANT_COLS = ["pm25_ugm3", "pm10_ugm3", "no2_ppb", "so2_ppb", "co_ppm", "o3_ppb"]
