from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
import json
from . import models, schemas
import time
from datetime import datetime, timedelta

def get_sensor(db: Session, sensor_id: int, mongodb_client: Session) -> Optional[models.Sensor]:
    
    mongodb_client.getDatabase("mydatabase")
    mycol = mongodb_client.getCollection("sensors")
    doc = mycol.find_one({"id": sensor_id})
    if doc:
        del doc['_id']
    return doc

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_elasticsearch(es: Session):
    es_index_name='index_sensors'
    es.create_index(es_index_name)
    mapping = {
        'properties': {
            'name': {'type': 'text'},
            'description': {'type': 'text'},
            'type': {'type': 'text'}
        }
    }
    es.create_mapping(es_index_name,mapping)

    
def create_sensor(db: Session, sensor: schemas.SensorCreate, mongodb_client: Session, cassandra: Session, es: Session) -> models.Sensor:
    if len(get_sensors(db)) == 0:
        create_elasticsearch(es)
    
    if len(get_sensors(db)) == 0:
        cassandra.execute("CREATE KEYSPACE sensor WITH replication =  {'class': 'SimpleStrategy', 'replication_factor' : 3};")
        cassandra.execute("CREATE TABLE sensor.temperature_values (id int, value_id int, temperature decimal, PRIMARY KEY (id, value_id)) WITH comment = 'Find values temperature'")
        cassandra.execute("CREATE TABLE sensor.types (type text PRIMARY KEY, count counter) WITH comment = 'Find number of types of sensor';")
        cassandra.execute("CREATE TABLE sensor.battery (id int PRIMARY KEY, battery_level decimal) WITH comment = 'Find sensor with battery level <20%';")


    # Puting data in database
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    
    # Puting data in elasticsearch
    es_index_name='index_sensors'
    es_doc={
            'name': sensor.name,
            'description': sensor.description,
            'type': sensor.type
        }
    es.index_document(es_index_name, es_doc)

    # Puting data in cassandra
    cassandra.execute("UPDATE sensor.types SET count = count + 1 WHERE type = %s;", (sensor.type,))

    
    # Puting data in mongodb
    mongodb_client.getDatabase("mydatabase")
    mycol = mongodb_client.getCollection("sensors")

    mydoc = {
        "id": db_sensor.id
    }
    for key, value in sensor.dict().items():
        mydoc[key] = value
        
    mycol.insert_one(mydoc)

    doc = mycol.find_one({"id": db_sensor.id})
    if doc:
        del doc['_id']
        
    return doc


def record_data(redis: Session, sensor_id: int, data: schemas.SensorData, cassandra: Session, timescale: Session) -> schemas.Sensor:
    
    # Save data in table
    query = """
        INSERT INTO sensor_data (id, velocity, temperature, humidity, battery_level, last_seen)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    timescale.execute(query, (sensor_id, data.velocity, data.temperature, data.humidity, data.battery_level, data.last_seen))
    
    # Save data in cassandra
    result_set = cassandra.execute("SELECT * FROM sensor.battery WHERE id = %s;", (sensor_id,))
    existing_row = result_set.one() if not result_set.current_rows == 0 else None

    if existing_row and data.battery_level < 0.2:
        cassandra.execute("UPDATE sensor.battery SET battery_level = %s WHERE id = %s;", (data.battery_level, sensor_id))
    elif not existing_row and data.battery_level < 0.2:
        cassandra.execute("INSERT INTO sensor.battery (id, battery_level) VALUES (%s, %s);", (sensor_id, data.battery_level))

         
    if data.temperature != None:
        value_id = cassandra.execute("SELECT COUNT(*) AS num_rows FROM sensor.temperature_values;")
        cassandra.execute("INSERT INTO sensor.temperature_values (id, value_id, temperature) VALUES (%s, %s, %s);", (sensor_id, value_id.one().num_rows, data.temperature))

    # Save data in redis
    db_sensordata = json.dumps(data.dict())
    return redis._client.set(sensor_id, db_sensordata)


def get_data(redis: Session, sensor_id: int, db: Session, timescale: Session, mongodb_client: Session, from_: str = None, to: str = None, bucket: str = None) -> schemas.Sensor:
    
    # Mirem el que ens estan demanant
    if to:
        dic = {'hour': '1 h', 'day': '1 day', 'week': '1 week', 'month': '1 month', 'year': '1 year'}
       
        # Fem les agregacions        
        query = """
            CREATE MATERIALIZED VIEW materialized_view
            WITH (timescaledb.continuous) AS
            SELECT
                id, 
                AVG(velocity) AS vel, 
                AVG(temperature) AS temp, 
                AVG(humidity) AS hum,
                MIN(battery_level) AS bat, 
                time_bucket(%s, last_seen) AS time
            FROM
                sensor_data
            GROUP BY
               id,  time_bucket(%s, last_seen);
        """
        timescale.execute(query, (dic[bucket], dic[bucket]))
        
        # Agafem les dades de l'interval de temps que volem
        
        query = """
            SELECT * FROM materialized_view
            WHERE time >= %s
            AND time <= %s
            AND id = %s;
        """
        if bucket == 'week':
            # When we do the week table, all the dates go back two days 
            date_object = datetime.fromisoformat(from_[:-1])
            two_days_ago_from = date_object - timedelta(days=2)
            two_days_ago_string_from = two_days_ago_from.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
            date_object = datetime.fromisoformat(to[:-1]) 
            two_days_ago_to = date_object - timedelta(days=2)
            two_days_ago_string_to = two_days_ago_to.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"

            timescale.execute(query, (two_days_ago_string_from, two_days_ago_string_to, sensor_id))
        else: 
            timescale.execute(query, (from_, to, sensor_id))
            
        data = timescale.getCursor().fetchall()

        # Eliminem la taula
        query = """
            DROP MATERIALIZED VIEW materialized_view;
        """
        timescale.execute(query) 

        return data
    else:
        data_str = redis._client.get(sensor_id)

        decoded_data =data_str.decode()
        db_sensordata = json.loads(decoded_data)
        db_sensordata['id'] = sensor_id
        db_sensordata['name'] = get_sensor(db, sensor_id, mongodb_client)['name']
        return db_sensordata
    
    

def delete_sensor(db: Session, sensor_id: int, mongodb_client: Session, es: Session):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()

    mongodb_client.getDatabase("mydatabase")
    mycol = mongodb_client.getCollection("sensors")
    mycol.delete_one({"id": sensor_id})

    es_index_name='index_sensors'
    query = {
            'query': {
                'term': {
                    'name.keyword': db_sensor.name
                }
            }
        }
    
    results = es.search(es_index_name, query)
    for hit in results['hits']['hits']:
        index_name = hit['_id']
        es.client.delete(index=es_index_name, id=index_name)
    return db_sensor

def get_sensors_near(mongodb: Session, latitude: float, longitude: float, radius: float, db: Session, redis: Session):
    mongodb.getDatabase("mydatabase")
    mycol = mongodb.getCollection("sensors")
    query = {
        "latitude": {"$gte": latitude - radius, "$lte": latitude + radius},
        "longitude": {"$gte": longitude - radius, "$lte": longitude + radius}
    }
    sensors = mycol.find(query)
    dataSensors = []
    for sensor in sensors:
        db_sensor = get_sensor(db, sensor['id'], mongodb)
        data_sensor = get_data(redis, sensor['id'], db_sensor)
        dataSensors.append(data_sensor)
    
    return json.dumps(dataSensors, indent=4)

def search_sensors(db: Session, mongodb: Session, es: Session, query: str, size:int, search_type: str):
    time.sleep(1)
    es_index_name='index_sensors'

    query_dict = json.loads(query)
    key, value = next(iter(query_dict.items()))

    if search_type == 'prefix':
        query = {
            'query': {
                'prefix': {
                    key:   {
                        'value' : value,
                        'case_insensitive': True
                    }
                }
            }
        }
    elif search_type == 'similar':
        query = {
            'query': {
                'match': {
                    key:  {
                        'query' : value,
                        'fuzziness': 'AUTO',
                        'operator': 'and',
                        'zero_terms_query': 'all'
                    }
                }
            }
        }
    else:
        query = {
            'query': {
                search_type: {
                    key:    value
                }
            }
        }
        
    results = es.search(es_index_name, query)
    
    sensors = []
    count = 0
    for hit in results['hits']['hits']:
        sensor = get_sensor_by_name(db, hit['_source']['name'])
        data_sensor = get_sensor(db, sensor.id, mongodb)
        if data_sensor not in sensors and count < size:
            sensors.append(data_sensor)
            count += 1

    return json.dumps(sensors, indent=4)


def get_sensors_quantity(db: Session, cassandra: Session):
    query = """
        SELECT type, count FROM sensor.types GROUP BY type;
    """
    data = cassandra.execute(query)

    sensors = []

    for row in data:
        sensor = {"type": row.type, "quantity": row.count}
        sensors.append(sensor)

    return {"sensors": sensors}

def get_low_battery_sensors(db: Session, cassandra: Session, mongodb_client: Session):
    query = """
        SELECT id, battery_level FROM sensor.battery;
    """
    data = cassandra.execute(query)

    sensors = []

    for row in data:
        data_sensor = get_sensor(db, row.id, mongodb_client)
        data_sensor["battery_level"] = row.battery_level
        sensors.append(data_sensor)

    return {"sensors": sensors}

def get_temperature_values(db: Session, cassandra: Session, mongodb_client: Session):
    query = """
        SELECT id, MIN(temperature) AS min, MAX(temperature) AS max, AVG(temperature) AS avg FROM sensor.temperature_values GROUP BY  id;
    """
    data = cassandra.execute(query)
    
    sensors = []

    for row in data:
        temp_data = {}
        temp_data["max_temperature"] = row.max
        temp_data["min_temperature"] = row.min
        temp_data["average_temperature"] = row.avg

        data_sensor = get_sensor(db, row.id, mongodb_client)
        data_sensor["values"] = [temp_data]

        sensors.append(data_sensor)
        
    return {"sensors": sensors}