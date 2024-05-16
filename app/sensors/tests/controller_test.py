from fastapi.testclient import TestClient
import pytest
from app.main import app
from shared.redis_client import RedisClient
from shared.mongodb_client import MongoDBClient
from shared.cassandra_client import CassandraClient
from app.elasticsearch_client import ElasticsearchClient
from app.timescale import Timescale
import time

client = TestClient(app)

@pytest.fixture(scope="session", autouse=True)
def clear_dbs():
     from shared.database import engine
     from shared.sensors import models
     models.Base.metadata.drop_all(bind=engine)
     models.Base.metadata.create_all(bind=engine)
     redis = RedisClient(host="redis")
     redis.clearAll()
     redis.close()
     mongo = MongoDBClient(host="mongodb")
     mongo.clearDb("sensors")
     mongo.close()
     es = ElasticsearchClient(host="elasticsearch")
     es.clearIndex("sensors")  
     ts = Timescale()
     ts.execute("DROP TABLE IF EXISTS sensor_data")
     ts.close()

     while True:
        try:
            cassandra = CassandraClient(["cassandra"])
            cassandra.get_session().execute("DROP KEYSPACE IF EXISTS sensor")
            cassandra.close()
            break
        except Exception as e:
            time.sleep(2)


# Test pressent a les pràctiques: Columnars, Temporals, Indexos, Documental
def test_create_sensor_temperatura_1():
    """A sensor can be properly created"""
    response = client.post("/sensors", json={"name": "Sensor Temperatura 1", "latitude": 1.0, "longitude": 1.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:00", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy"})
    assert response.status_code == 200
    assert response.json() == {"id": 1, "name": "Sensor Temperatura 1", "latitude": 1.0, "longitude": 1.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:00", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy"}

# Test pressent a les pràctiques: Columnars, Temporals, Indexos, Documental
def test_create_sensor_velocitat_1():
    response = client.post("/sensors", json={"name": "Velocitat 1", "latitude": 1.0, "longitude": 1.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:01", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 1"})
    assert response.status_code == 200
    assert response.json() == {"id": 2, "name": "Velocitat 1", "latitude": 1.0, "longitude": 1.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:01", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 1"}

# Test pressent a les pràctiques: Documental
def test_redis_connection():
    redis_client = RedisClient(host="redis")
    assert redis_client.ping()
    redis_client.close()

# Test pressent a les pràctiques: Documental
def test_mongodb_connection():
    mongodb_client = MongoDBClient(host="mongodb")
    assert mongodb_client.ping()
    mongodb_client.close()

# Test pressent a les pràctiques: Documental
def test_post_sensor_data_not_exists():
    response = client.post("/sensors/3/data", json={"temperature": 1.0, "humidity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-01T00:00:00.000Z"})
    assert response.status_code == 404
    assert "Sensor not found" in response.text

# Test pressent a les pràctiques: Documental
def test_get_sensor_data_not_exists():
    response = client.get("/sensors/3/data")
    assert response.status_code == 404
    assert "Sensor not found" in response.text

# Test pressent a les pràctiques: Columnars, Temporals, Indexos
def test_create_sensor_velocitat_2():
    response = client.post("/sensors", json={"name": "Velocitat 2", "latitude": 2.0, "longitude": 2.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:02", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 2"})
    assert response.status_code == 200
    assert response.json() == {"id": 3, "name": "Velocitat 2", "latitude": 2.0, "longitude": 2.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:02", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 2"}

# Test pressent a les pràctiques: Indexos
def test_get_sensor_1():
    """A sensor can be properly retrieved"""
    response = client.get("/sensors/1")
    assert response.status_code == 200
    assert response.json() == {"id": 1, "name": "Sensor Temperatura 1", "latitude": 1.0, "longitude": 1.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:00", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy"}

# Test pressent a les pràctiques: Indexos
def test_get_sensor_2():
    """A sensor can be properly retrieved"""
    response = client.get("/sensors/2")
    assert response.status_code == 200
    assert response.json() == {"id": 2, "name": "Velocitat 1", "latitude": 1.0, "longitude": 1.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:01", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 1"}

# Test pressent a les pràctiques: Indexos
def test_get_sensor_3():
    """A sensor can be properly retrieved"""
    response = client.get("/sensors/3")
    assert response.status_code == 200
    assert response.json() == {"id": 3, "name": "Velocitat 2", "latitude": 2.0, "longitude": 2.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:02", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 2"}

# Test pressent a les pràctiques: Temporals
def test_post_sensor_data_not_exists():
    response = client.post("/sensors/4/data", json={"temperature": 1.0, "humidity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-01T00:00:00.000Z"})
    assert response.status_code == 404
    assert "Sensor not found" in response.text

# Test pressent a les pràctiques: Temporals
def test_get_sensor_data_not_exists():
    response = client.get("/sensors/4/data")
    assert response.status_code == 404
    assert "Sensor not found" in response.text

# Test pressent a les pràctiques: Columnars
def test_create_sensor_temperatura_2():
    """A sensor can be properly created"""
    response = client.post("/sensors", json={"name": "Sensor Temperatura 2", "latitude": 2.0, "longitude": 2.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:03", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy"})
    assert response.status_code == 200
    assert response.json() == {"id": 4, "name": "Sensor Temperatura 2", "latitude": 2.0, "longitude": 2.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:03", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy"}

# Test pressent a les pràctiques: Temporals
def test_post_sensor_data_dia_2():
    response = client.post("/sensors/1/data", json={"temperature": 15.0, "humidity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-02T00:00:00.000Z"})
    assert response.status_code == 200

# Test pressent a les pràctiques: Temporals
def test_post_sensor_data_dia_3():
    response = client.post("/sensors/1/data", json={"temperature": 18.0, "humidity": 1.0, "battery_level": 0.9, "last_seen": "2020-01-03T00:00:00.000Z"})
    assert response.status_code == 200

# Test pressent a les pràctiques: Columnars, Documental
def test_post_sensor_data_temperatura_2():
    response = client.post("/sensors/1/data", json={"temperature": 4.0, "humidity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-01T00:00:00.000Z"})
    assert response.status_code == 200

# Test pressent a les pràctiques: Documental
def test_get_sensor_1_data():
    """We can get a sensor by its id"""
    response = client.get("/sensors/1/data")
    assert response.status_code == 200
    json = response.json()
    assert json["id"] == 1
    assert json["name"] == "Sensor Temperatura 1"
    assert json["temperature"] == 4.0
    assert json["humidity"] == 1.0
    assert json["battery_level"] == 1.0
    assert json["last_seen"] == "2020-01-01T00:00:00.000Z"

# Test pressent a les pràctiques: Columnars, Temporals, Documental
def test_post_sensor_data_temperatura_1():
    response = client.post("/sensors/1/data", json={"temperature": 1.0, "humidity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-01T00:00:00.000Z"})
    assert response.status_code == 200

# Test pressent a les pràctiques: Documental
def test_get_sensor_1_data_update():
    """We can get a sensor by its id"""
    response = client.get("/sensors/1/data")
    assert response.status_code == 200
    json = response.json()
    assert json["id"] == 1
    assert json["name"] == "Sensor Temperatura 1"
    assert json["temperature"] == 1.0
    assert json["humidity"] == 1.0
    assert json["battery_level"] == 1.0
    assert json["last_seen"] == "2020-01-01T00:00:00.000Z"

# Test pressent a les pràctiques: Columnars
def test_post_sensor_data_temperatura_3():
    response = client.post("/sensors/4/data", json={"temperature": 15.0, "humidity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-02T00:00:00.000Z"})
    assert response.status_code == 200

# Test pressent a les pràctiques: Columnars
def test_post_sensor_data_temperatura_4():
    response = client.post("/sensors/4/data", json={"temperature": 17.0, "humidity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-02T00:00:00.000Z"})
    assert response.status_code == 200

# Test pressent a les pràctiques: Temporals
def test_post_sensor_data_veolicitat_hora_1():
    response = client.post("/sensors/2/data", json={"velocity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-01T00:00:00.000Z"})
    assert response.status_code == 200

# Test pressent a les pràctiques: Temporals
def test_post_sensor_data_veolicitat_hora_2():
    response = client.post("/sensors/2/data", json={"velocity": 15.0, "battery_level": 1.0, "last_seen": "2020-01-01T01:00:00.000Z"})
    assert response.status_code == 200

# Test pressent a les pràctiques: Columnars, Documental
def test_post_sensor_data_veolicitat_1():
    response = client.post("/sensors/2/data", json={"velocity": 1.0, "battery_level": 0.1, "last_seen": "2020-01-01T00:00:00.000Z"})
    assert response.status_code == 200

# Test pressent a les pràctiques: Documental
def test_get_sensor_2_data():
    """We can get a sensor by its id"""
    response = client.get("/sensors/2/data")
    assert response.status_code == 200
    json = response.json()
    assert json["id"] == 2
    assert json["name"] == "Velocitat 1"
    assert json["velocity"] == 1.0
    assert json["battery_level"] == 0.1
    assert json["last_seen"] == "2020-01-01T00:00:00.000Z"

# Test pressent a les pràctiques: Temporals, Documental
def test_post_sensor_data_veolicitat_hora_3():
    response = client.post("/sensors/2/data", json={"velocity": 18.0, "battery_level": 0.9, "last_seen": "2020-01-01T02:00:00.000Z"})
    assert response.status_code == 200

# Test pressent a les pràctiques: Documental
def test_get_sensor_2_data_update():
    """We can get a sensor by its id"""
    response = client.get("/sensors/2/data")
    assert response.status_code == 200
    json = response.json()
    assert json["id"] == 2
    assert json["name"] == "Velocitat 1"
    assert json["velocity"] == 18.0
    assert json["battery_level"] == 0.9
    assert json["last_seen"] == "2020-01-01T02:00:00.000Z"

# Test pressent a les pràctiques: Temporals
def test_post_sensor_data_veolicitat_week_1():
    response = client.post("/sensors/3/data", json={"velocity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-01T00:00:00.000Z"})
    assert response.status_code == 200

# Test pressent a les pràctiques: Temporals
def test_post_sensor_data_veolicitat_week_2():
    response = client.post("/sensors/3/data", json={"velocity": 15.0, "battery_level": 1.0, "last_seen": "2020-01-08T00:00:00.000Z"})
    assert response.status_code == 200

# Test pressent a les pràctiques: Temporals
def test_post_sensor_data_veolicitat_week_3():
    response = client.post("/sensors/3/data", json={"velocity": 18.0, "battery_level": 0.9, "last_seen": "2020-01-15T00:00:00.000Z"})
    assert response.status_code == 200

# Test pressent a les pràctiques: Columnars
def test_post_sensor_data_veolicitat_2():
    response = client.post("/sensors/3/data", json={"velocity": 15.0, "battery_level": 0.15, "last_seen": "2020-01-01T01:00:00.000Z"})
    assert response.status_code == 200

# Test pressent a les pràctiques: Columnars
def test_get_values_sensor_temperatura():
    response = client.get("/sensors/temperature/values")
    assert response.status_code == 200
    assert response.json() == {"sensors": [{"id": 1, "name": "Sensor Temperatura 1", "latitude": 1.0, "longitude": 1.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:00", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy", "values": [{"max_temperature": 18.0, "min_temperature": 1.0, "average_temperature": 9.5}]}, {"id": 4, "name": "Sensor Temperatura 2", "latitude": 2.0, "longitude": 2.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:03", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy", "values": [{"max_temperature": 17.0, "min_temperature": 15.0, "average_temperature": 16.0}]}]}

# Test pressent a les pràctiques: Columnars
def test_get_sensors_quantity():
    response = client.get("/sensors/quantity_by_type")
    assert response.status_code == 200
    assert response.json() == {"sensors": [{"type":"Temperatura", "quantity": 2}, {"type":"Velocitat", "quantity": 2}]}

# Test pressent a les pràctiques: Columnars
def test_get_sensors_low_battery():
    response = client.get("/sensors/low_battery")
    assert response.status_code == 200
    assert response.json() == {"sensors": [{"id": 2, "name": "Velocitat 1", "latitude": 1.0, "longitude": 1.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:01", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 1", "battery_level": 0.1}, {"id": 3, "name": "Velocitat 2", "latitude": 2.0, "longitude": 2.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:02", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 2", "battery_level": 0.15}]}

# Test pressent a les pràctiques: Temporals
def test_get_sensor_data_1_day():
    """We can get a sensor by its id"""
    response = client.get("/sensors/1/data?from_=2020-01-01T00:00:00.000Z&to=2020-01-03T00:00:00.000Z&bucket=day")
    assert response.status_code == 200
    json = response.json()
    assert len(json) == 3

# Test pressent a les pràctiques: Temporals
def test_get_sensor_data_1_week():
    response = client.get("/sensors/1/data?from_=2020-01-01T00:00:00.000Z&to=2020-01-07T00:00:00.000Z&bucket=week")
    assert response.status_code == 200
    json = response.json()
    assert len(json) == 1

# Test pressent a les pràctiques: Temporals
def test_get_sensor_data_2_hour():
    response = client.get("/sensors/2/data?from_=2020-01-01T00:00:00.000Z&to=2020-01-01T02:00:00.000Z&bucket=hour")
    assert response.status_code == 200
    json = response.json()
    assert len(json) == 3

# Test pressent a les pràctiques: Temporals
def test_get_sensor_data_2_day():
    response = client.get("/sensors/2/data?from_=2020-01-01T00:00:00.000Z&to=2020-01-02T00:00:00.000Z&bucket=day")
    assert response.status_code == 200
    json = response.json()
    assert len(json) == 1

# Test pressent a les pràctiques: Temporals
def test_get_sensor_data_3_week():
    response = client.get("/sensors/3/data?from_=2020-01-01T00:00:00.000Z&to=2020-01-15T00:00:00.000Z&bucket=week")
    assert response.status_code == 200
    json = response.json()
    assert len(json) == 3

# Test pressent a les pràctiques: Temporals
def test_get_sensor_data_3_month():
    response = client.get("/sensors/3/data?from_=2020-01-01T00:00:00.000Z&to=2020-01-31T00:00:00.000Z&bucket=month")
    assert response.status_code == 200
    json = response.json()
    assert len(json) == 1
    
# Test pressent a les pràctiques: Indexos
def test_elasticsearch_client():
    """Elasticsearch client can be properly created"""
    es = ElasticsearchClient(host="elasticsearch")
    assert es.ping()
    # Create the index
    es_index_name='my_index'
    es.create_index(es_index_name)
    # Define the mapping for the index
    mapping = {
        'properties': {
            'title': {'type': 'text'},
            'description': {'type': 'text'},
            'price': {'type': 'float'}
        }
    }
    es.create_mapping('my_index',mapping)
    es_doc={
        'title': 'Test document',
        'description': 'This is a test document',
        'price': 10.0
    }
    es.index_document(es_index_name, es_doc)
    # Define the search query
    query = {
        'query': {
            'match': {
                'title': 'Test document'
            }
        }
    }
    # Perform the search and get the results
    results = es.search(es_index_name, query)

    # Loop through the results and print the title and price of each document
    for hit in results['hits']['hits']:
        print(f"Title: {hit['_source']['title']}")
        print(f"Price: {hit['_source']['price']}")
        assert hit['_source']['title'] == 'Test document'
        assert hit['_source']['price'] == 10.0
    
    # Delete the index
    es.clearIndex(es_index_name)
    es.close()

# Test pressent a les pràctiques: Indexos
def test_search_sensors_temperatura():
    """Sensors can be properly searched by type"""
    response = client.get('/sensors/search?query={"type":"Temperatura"}')
    assert response.status_code == 200
    assert response.json() == [{"id": 1, "name": "Sensor Temperatura 1", "latitude": 1.0, "longitude": 1.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:00", "manufacturer": "Dummy", "model": "Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy"},
                               {"id": 4, "name": "Sensor Temperatura 2", "latitude": 2.0, "longitude": 2.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:03", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy"}]

# Test pressent a les pràctiques: Indexos
def test_search_sensors_name_similar():
    """Sensors can be properly searched by name"""
    response = client.get('/sensors/search?query={"name":"Velocidad 1"}&search_type=similar')
    assert response.status_code == 200
    assert response.json() == [{"id": 2, "name": "Velocitat 1", "latitude": 1.0, "longitude": 1.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:01", "manufacturer": "Dummy", "model": "Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 1"}]

# Test pressent a les pràctiques: Indexos
def test_search_sensors_name_prefix():
    """Sensors can be properly searched by name"""
    response = client.get('/sensors/search?query={"name":"Veloci"}&search_type=prefix')
    assert response.status_code == 200
    assert response.json() == [
        {"id": 2, "name": "Velocitat 1", "latitude": 1.0, "longitude": 1.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:01", "manufacturer": "Dummy", "model": "Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 1"},
        {"id": 3, "name": "Velocitat 2", "latitude": 2.0, "longitude": 2.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:02", "manufacturer": "Dummy", "model": "Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 2"}]
    
# Test pressent a les pràctiques: Indexos
def test_search_sensors_type_limit():
    """Sensors can be properly searched by type"""
    response = client.get('/sensors/search?query={"type":"Velocitat"}&size=1')
    assert response.status_code == 200
    assert len(response.json()) == 1

# Test pressent a les pràctiques: Indexos
def test_search_sensors_description_similar():
    """Sensors can be properly searched by description"""
    response = client.get('/sensors/search?query={"description":"dummy"}&search_type=similar')
    assert response.status_code == 200
    assert response.json() == [{"id": 1, "name": "Sensor Temperatura 1", "latitude": 1.0, "longitude": 1.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:00", "manufacturer": "Dummy", "model": "Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy"},
        {"id": 4, "name": "Sensor Temperatura 2", "latitude": 2.0, "longitude": 2.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:03", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy"},
        {"id": 2, "name": "Velocitat 1", "latitude": 1.0, "longitude": 1.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:01", "manufacturer": "Dummy", "model": "Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 1"},
        {"id": 3, "name": "Velocitat 2", "latitude": 2.0, "longitude": 2.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:02", "manufacturer": "Dummy", "model": "Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 2"}]  
    
# Test pressent a les pràctiques: Documental
def test_get_near():
    response = client.get("/sensors/near?latitude=1.0&longitude=1.0&radius=1")
    assert response.status_code == 200
    json = response.json()
    assert json[0]["id"] == 1
    assert json[0]["name"] == "Sensor Temperatura 1"
    assert json[0]["temperature"] == 1.0
    assert json[0]["humidity"] == 1.0
    assert json[0]["battery_level"] == 1.0
    assert json[0]["last_seen"] == "2020-01-01T00:00:00.000Z"
    assert json[1]["id"] == 2
    assert json[1]["name"] == "Velocitat 1"
    assert json[1]["velocity"] == 18.0
    assert json[1]["battery_level"] == 0.9
    assert json[1]["last_seen"] == "2020-01-01T02:00:00.000Z"

# Test pressent a les pràctiques: Documental
def test_delete_sensor_1():
    response = client.delete("/sensors/1")
    assert response.status_code == 200

# Test pressent a les pràctiques: Documental
def test_delete_sensor_2():
    response = client.delete("/sensors/2")
    assert response.status_code == 200