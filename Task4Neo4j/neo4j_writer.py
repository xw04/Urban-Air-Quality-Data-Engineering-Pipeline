# Author: Lee Qian Hui
from neo4j import GraphDatabase, basic_auth
from pyspark.sql import DataFrame

class Neo4jWriter:
    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_pass: str):
        self.neo4j_uri = neo4j_uri
        self.neo4j_user = neo4j_user
        self.neo4j_pass = neo4j_pass

    def _neo4j_session(self):
        driver = GraphDatabase.driver(self.neo4j_uri, auth=basic_auth(self.neo4j_user, self.neo4j_pass))
        return driver, driver.session(database=None) 

    def write_graph(self, df: DataFrame) -> None:
        def send_partition(rows):
            driver = GraphDatabase.driver(self.neo4j_uri, auth=basic_auth(self.neo4j_user, self.neo4j_pass))
            cypher = """
                UNWIND $batch AS row
                MERGE (c:City {key: row.city}) 
                  ON CREATE SET c.name = row.city, c.country = row.country, c.hemisphere = row.hemisphere 
                  
                MERGE (d:Day {key: row.event_date})
                  ON CREATE SET d.date = row.event_date, 
                        d.year = row.year, 
                        d.month = row.month, 
                        d.day = row.day, 
                        d.day_of_week = row.day_of_week, 
                        d.season = row.season

                MERGE (r:Reading {city: row.city, event_date: row.event_date})
                  ON CREATE SET r.aqi = row.AQI, 
                        r.pm25 = row.pm25_ugm3, 
                        r.pm10 = row.pm10_ugm3,
                        r.no2 = row.no2_ppb, 
                        r.so2 = row.so2_ppb, 
                        r.co = row.co_ppm, 
                        r.o3 = row.o3_ppb,
                        r.temperature = row.temp_c, 
                        r.humidity = row.humidity_pct, 
                        r.wind = row.wind_ms,
                        r.pm_ratio = row.pm_ratio

                MERGE (a:AQICategory {category: row.aqi_category})
                  ON CREATE SET a.category_name = row.aqi_category,
                    a.aqi_bucket_pm25 = row.aqi_bucket_pm25,
                    a.aqi_bucket_pm10 = row.aqi_bucket_pm10,
                    a.aqi_bucket_no2 = row.aqi_bucket_no2,
                    a.aqi_bucket_so2 = row.aqi_bucket_so2,
                    a.aqi_bucket_co = row.aqi_bucket_co,
                    a.aqi_bucket_o3 = row.aqi_bucket_o3

                MERGE (r)-[:HAS_CATEGORY]->(a)
                MERGE (c)-[:HAS_READING]->(r)
                MERGE (r)-[:ON_DAY]->(d)
                
                MERGE (e:Compliant {key: row.city + '-' + row.event_date})
                  ON CREATE SET e.pm25_exceeds_who = row.pm25_exceeds_who, 
                    e.pm10_exceeds_who = row.pm10_exceeds_who,
                    e.no2_exceeds_who = row.no2_exceeds_who, 
                    e.so2_exceeds_who = row.so2_exceeds_who,
                    e.co_exceeds_who = row.co_exceeds_who, 
                    e.o3_exceeds_who = row.o3_exceeds_who,
                    e.who_compliant_all = row.who_compliant_all
                MERGE (r)-[:HAS_COMPLIANT]->(e)
            """
            try:
                with driver.session() as session:
                    batch = []
                    for row in rows:
                        batch.append(row.asDict())
                        if len(batch) >= 500:
                            session.run(cypher, {"batch": batch})
                            batch.clear()
                    if batch:
                        session.run(cypher, {"batch": batch})
            except Exception as e:
                print(f"Error running Cypher query: {e}")
            finally:
                driver.close()

        df.repartition(8).foreachPartition(send_partition)
