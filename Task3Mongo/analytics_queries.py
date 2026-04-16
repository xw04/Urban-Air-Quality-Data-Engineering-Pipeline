# Author: Kam Win Ni
from pymongo_utils import PyMongoUtils

class DashboardQueries:
    def __init__(self, db: str = "aqi_demo", coll: str = "aq_readings"):
        self.db = db
        self.coll = coll
        self.pmu = PyMongoUtils()

    def seasonal_pm25_profile(self):

        pipeline = [
            {"$project": {
                "city": "$location.city",
                "season": "$calendar.season",
                "pm25": "$measurements.pollutants.pm25_ugm3",
                "season_order": {
                    "$switch": {
                        "branches": [
                            {"case": {"$eq": ["$calendar.season", "Winter"]}, "then": 1},
                            {"case": {"$eq": ["$calendar.season", "Spring"]}, "then": 2},
                            {"case": {"$eq": ["$calendar.season", "Summer"]}, "then": 3},
                            {"case": {"$eq": ["$calendar.season", "Autumn"]}, "then": 4},
                        ],
                        "default": 5
                    }
                }
            }},
            {"$match": {"pm25": {"$ne": None}, "season": {"$ne": None}}},
            {"$group": {
                "_id": {"city": "$city", "season": "$season", "ord": "$season_order"},
                "avg_pm25": {"$avg": "$pm25"}
            }},
            {"$project": {
                "_id": 0,
                "city": "$_id.city",
                "season": "$_id.season",
                "season_order": "$_id.ord",
                "avg_pm25": {"$round": ["$avg_pm25", 2]}
            }},
            {"$sort": {"city": 1, "season_order": 1}},
            {"$setWindowFields": {
                "partitionBy": "$city",
                "sortBy": {"avg_pm25": 1},
                "output": {"season_rank": {"$rank": {}}}
            }}
        ]
        return list(self.pmu.get_collection(self.db, self.coll).aggregate(pipeline))

    def monthly_avg_aqi_mom(self):
        pipeline = [
            {"$project": {
                "city": "$location.city",
                "ts": "$ts_utc",
                "aqi": "$measurements.aqi"
            }},
            {"$match": {"aqi": {"$ne": None}}},
            {"$addFields": {"month": {"$dateTrunc": {"date": "$ts", "unit": "month"}}}},
            {"$group": {
                "_id": {"city": "$city", "month": "$month"},
                "avg_aqi": {"$avg": "$aqi"}
            }},
            {"$project": {
                "_id": 0,
                "city": "$_id.city",
                "month": "$_id.month",
                "avg_aqi": {"$round": ["$avg_aqi", 2]}
            }},
            {"$sort": {"city": 1, "month": 1}},
            {"$setWindowFields": {
                "partitionBy": "$city",
                "sortBy": {"month": 1},
                "output": {"prev_avg": {"$shift": {"by": 1, "output": "$avg_aqi"}}}
            }},
            {"$project": {
                "city": 1,
                "month": 1,
                "avg_aqi": 1,
                "mom_change_aqi": {
                    "$cond": [
                        {"$and": [{"$ne": ["$prev_avg", None]}, {"$ne": ["$avg_aqi", None]}]},
                        {"$round": [{"$subtract": ["$avg_aqi", "$prev_avg"]}, 2]},
                        None
                    ]
                }
            }}
        ]
        return list(self.pmu.get_collection(self.db, self.coll).aggregate(pipeline))

    def run_and_print(self):
        print("\n-- Seasonal PM2.5 profile (ranked) --")
        for d in self.seasonal_pm25_profile():
            print(d)

        print("\n-- Monthly average AQI + MoM change --")
        for d in self.monthly_avg_aqi_mom():
            print(d)

if __name__ == "__main__":
    DashboardQueries().run_and_print()
