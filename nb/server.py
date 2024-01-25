import station_pb2_grpc, station_pb2
import cassandra
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
import grpc
from concurrent import futures
import traceback

class Record(object):
    def __init__(self, tmin, tmax):
        self.tmin = tmin
        self.tmax = tmax

class Station(station_pb2_grpc.StationServicer):
    def __init__(self):
        self.cluster = Cluster(['p6-db-1', 'p6-db-2', 'p6-db-3'])
        self.cass = self.cluster.connect()

    def RecordTemps(self, request, context):
        try:
            self.cluster.register_user_type('weather', 'station_record', Record)
            insert_statement = self.cass.prepare("""
                INSERT INTO weather.stations (id, date, record)
                VALUES (?, ?, ?)
            """)
            insert_statement.consistency_level = ConsistencyLevel.ONE
            self.cass.execute(insert_statement, (request.station, request.date, Record(request.tmin, request.tmax)))
            print("success")
            return station_pb2.RecordTempsReply(error="")

        except cassandra.Unavailable as e:
            return station_pb2.RecordTempsReply(error=f'need {e.required_replicas} replicas, but only have {e.alive_replicas}')
        
        except cassandra.cluster.NoHostAvailable as e:
            for value in e.errors:
                if value == cassandra.Unavailable:
                    return station_pb2.RecordTempsReply(error=f'need {value.required_replicas} replicas, but only have {value.alive_replicas}')

        except Exception as e:
            return station_pb2.RecordTempsReply(error=str(traceback.format_exc()))

    def StationMax(self, request, context):
        try:
            max_statement = self.cass.prepare("SELECT MAX(record.tmax) AS tmax FROM weather.stations WHERE id=?")
            max_statement.consistency_level = ConsistencyLevel.THREE
            result = self.cass.execute(max_statement, [request.station]).one().tmax
            print(result)

            return station_pb2.StationMaxReply(tmax=result, error="")

        except cassandra.Unavailable as e:
            return station_pb2.StationMaxReply(tmax=0, error=f'need {e.required_replicas} replicas, but only have {e.alive_replicas}')
        
        except cassandra.cluster.NoHostAvailable as e:
            for value in e.errors:
                if value == cassandra.Unavailable:
                    return station_pb2.StationMaxReply(tmax=0, error=f'need {value.required_replicas} replicas, but only have {value.alive_replicas}')

        except Exception as e:
            return station_pb2.StationMaxReply(tmax=0, error=str(e))

if __name__ == "__main__":
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1), options=(('grpc.so_reuseport', 0),))
    station_pb2_grpc.add_StationServicer_to_server(Station(), server)
    server.add_insecure_port("[::]:5440", )
    server.start()
    print("started")
    server.wait_for_termination()