# Author: Lee Qian Hui
from loader import Loader
from neo4j_writer import Neo4jWriter

class DataLoader:
    def __init__(self, neo4j_uri, neo4j_user, neo4j_pass, hdfs_path):
        self.neo4j_writer = Neo4jWriter(neo4j_uri, neo4j_user, neo4j_pass)
        self.air_quality_loader = Loader(hdfs_path)

    def load_data(self):
        df = self.air_quality_loader.load_from_hdfs()
        self.neo4j_writer.write_graph(df)
        print("Data successfully loaded into Neo4j.")


if __name__ == "__main__":
    neo4j_uri = "neo4j+s://fa5d78e1.databases.neo4j.io"
    neo4j_user = "neo4j"
    neo4j_pass = "t2l0UlomQbkdTBatbfIVsduhg9E2XRYljtkR5PFcT6A"

    hdfs_path = "hdfs://localhost:9000/user/student/air_quality_cleaned/valid_json/part-00000-*.json"

    data_loader = DataLoader(neo4j_uri, neo4j_user, neo4j_pass, hdfs_path)
    data_loader.load_data()
