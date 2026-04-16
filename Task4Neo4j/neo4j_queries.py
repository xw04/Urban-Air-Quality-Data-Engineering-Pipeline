#Author: Lee Qian Hui
from neo4j import GraphDatabase, basic_auth
from tabulate import tabulate

class Neo4jQueries:
    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_pass: str):
        self.neo4j_uri = neo4j_uri
        self.neo4j_user = neo4j_user
        self.neo4j_pass = neo4j_pass

    def _neo4j_session(self):
        driver = GraphDatabase.driver(
            self.neo4j_uri,
            auth=basic_auth(self.neo4j_user, self.neo4j_pass)
        )
        return driver, driver.session(database=None)  

    def query_top_cities_by_avg_aqi(self, start_date: str, end_date: str, top_k: int = 5):
        driver, session = self._neo4j_session()
        try:
            cypher = """
                MATCH (c:City)-[:HAS_READING]->(r:Reading)-[:ON_DAY]->(d:Day)
                WHERE d.date >= $start AND d.date <= $end
                WITH c.name AS city, c.country AS country, avg(r.aqi) AS avgAQI
                RETURN city, country, round(avgAQI, 2) AS avgAQI
                ORDER BY avgAQI DESC
                LIMIT $top_k
            """
            result = session.run(cypher, {"start": start_date, "end": end_date, "top_k": top_k})
            return [(record["city"], record["country"], record["avgAQI"]) for record in result]
        finally:
            session.close()
            driver.close()
            
    def query_pollution_driver_per_city(self,
                                    start_date: str = "2024-01-01",
                                    end_date: str = "2024-12-31",
                                    limit: int = 10):
        driver, session = self._neo4j_session()
        try:
            cypher = """
                MATCH (c:City)-[:HAS_READING]->(r:Reading)-[:ON_DAY]->(d:Day)
                OPTIONAL MATCH (r)-[:HAS_COMPLIANT]->(e:Compliant)
                WHERE d.date >= $start AND d.date <= $end
                WITH c.name AS city,
                     sum(CASE WHEN e.pm25_exceeds_who = 'Yes' THEN 1 ELSE 0 END) AS pm25,
                     sum(CASE WHEN e.pm10_exceeds_who = 'Yes' THEN 1 ELSE 0 END) AS pm10,
                     sum(CASE WHEN e.no2_exceeds_who  = 'Yes' THEN 1 ELSE 0 END) AS no2,
                     sum(CASE WHEN e.so2_exceeds_who  = 'Yes' THEN 1 ELSE 0 END) AS so2,
                     sum(CASE WHEN e.co_exceeds_who   = 'Yes' THEN 1 ELSE 0 END) AS co,
                     sum(CASE WHEN e.o3_exceeds_who   = 'Yes' THEN 1 ELSE 0 END) AS o3
                WITH city, pm25, pm10, no2, so2, co, o3,
                     (pm25 + pm10 + no2 + so2 + co + o3) AS total_exceed
                RETURN city, pm25, pm10, no2, so2, co, o3, total_exceed
                ORDER BY total_exceed DESC, city ASC
                LIMIT $limit
            """
            params = {"start": start_date, "end": end_date, "limit": limit}
            result = session.run(cypher, params)
            rows = []
            for rec in result:
                rows.append({
                    "city": rec["city"],
                    "pm25": rec["pm25"], "pm10": rec["pm10"], "no2": rec["no2"],
                    "so2": rec["so2"],   "co": rec["co"],     "o3": rec["o3"],
                    "total": rec["total_exceed"],
                })
            return rows
        finally:
            session.close()
            driver.close()

if __name__ == "__main__":
    neo4j_uri = "neo4j+s://fa5d78e1.databases.neo4j.io"
    neo4j_user = "neo4j"
    neo4j_pass = "t2l0UlomQbkdTBatbfIVsduhg9E2XRYljtkR5PFcT6A"

    queries = Neo4jQueries(neo4j_uri, neo4j_user, neo4j_pass)
    
    top_cities = queries.query_top_cities_by_avg_aqi("2024-01-01", "2024-12-31")
    print("Top Cities by Avg AQI:")
    print(tabulate(top_cities, headers=["City", "Country", "Avg AQI"], tablefmt="pretty"))

    top_pollution_driver = queries.query_pollution_driver_per_city(start_date="2024-01-01",
    end_date="2024-12-31",limit=9)
    print("\nTop Pollution Driver per City in 2024:")
    grouped_table = []
    for rec in  top_pollution_driver:
        city = rec["city"]
        items = [
            ("pm25", rec["pm25"]),
            ("pm10", rec["pm10"]),
            ("no2",  rec["no2"]),
            ("o3",   rec["o3"]),
            ("so2",  rec["so2"]),
            ("co",   rec["co"]),
        ]
        items.sort(key=lambda x: x[1], reverse=True)

        first = True
        for pol, days in items:
            grouped_table.append([city if first else "", pol, days])
            first = False
    
    from tabulate import tabulate
    print(tabulate(grouped_table, headers=["City", "Pollutant", "Severe Days"], tablefmt="pretty"))
