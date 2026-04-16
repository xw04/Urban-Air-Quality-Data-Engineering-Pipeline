# Author: Kam Win Ni
from pymongo_utils import PyMongoUtils

class IndexBootstrap:
    def __init__(self, db_name: str = "aqi_demo", coll_name: str = "aq_readings"):
        self.db_name = db_name
        self.coll_name = coll_name
        self.pmu = PyMongoUtils()

    def run(self):
        db = self.pmu.get_database(self.db_name)
        if self.coll_name not in db.list_collection_names():
            db.create_collection(
                self.coll_name,
                timeseries={"timeField": "ts_utc", "metaField": "location", "granularity": "hours"}
            )
        coll = db[self.coll_name]
        coll.create_index([("location.city", 1), ("ts_utc", -1)])
        coll.create_index([("location.country", 1), ("ts_utc", -1)])
        coll.create_index([("categories.aqi_category", 1), ("ts_utc", -1)])
        coll.create_index([("dedup", 1)], unique=True)
        coll.create_index([("calendar.season", 1), ("location.city", 1)])
        coll.create_index([("calendar.month", 1), ("location.city", 1)])

        print("Time-series collection and indexes are ready.")

if __name__ == "__main__":
    IndexBootstrap().run()
