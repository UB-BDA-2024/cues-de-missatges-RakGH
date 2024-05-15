from cassandra.cluster import Cluster

class CassandraClient:
    def __init__(self, hosts):
        self.cluster = Cluster(hosts,protocol_version=4)
        self.session = self.cluster.connect()

    def get_session(self):
        return self.session

    def close(self):
        self.cluster.shutdown()

    def execute(self, query, params=None):
        if params:
            return self.get_session().execute(query, params)
        else:
            return self.get_session().execute(query)
