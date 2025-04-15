import pika
import json
import psycopg2
from decouple import config
from datetime import datetime
from supabase import create_client
import asyncio
from supabase import create_async_client
from threading import Thread
import logging
from typing import Dict, Any
import requests
from requests.auth import HTTPBasicAuth

class FaultDetectionAgent:
    def __init__(self):
        # Configure logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        # RabbitMQ connection parameters
        self.RABBITMQ_HOST = 'localhost' if config('ENVIRONMENT', default='local') == 'local' else config('RABBITMQ_HOST')
        self.EXCHANGE_NAME = config('EXCHANGE_NAME', default='hotel_iot')
        self.ALERT_EXCHANGE_NAME = config('ALERT_EXCHANGE_NAME', default='fault_alerts')

        # Initialize Supabase client
        self.SUPABASE_URL = config('SUPABASE_URL')
        self.SUPABASE_API_KEY = config('SUPABASE_API_KEY')
        self.supabase = create_client(self.SUPABASE_URL, self.SUPABASE_API_KEY)

        self.fault_thresholds = {} # Global variable to store current thresholds
        self.last_occupied = {} # In-memory state to track last occupancy time (for stuck occupied detection)

        # TimescaleDB connection
        self.DB_NAME = config('TIMESCALEDB_DATABASE')
        self.DB_USER = config('TIMESCALEDB_USER')
        self.DB_PASSWORD = config('TIMESCALEDB_PASSWORD')
        self.DB_PORT = config('TIMESCALEDB_PORT', default='5432')
        self.DB_HOST = config('TIMESCALEDB_HOST') if config('ENVIRONMENT', default='local') == 'docker' else 'localhost'


    def init_db_connection(self):
        self.conn = psycopg2.connect(
            dbname=self.DB_NAME,
            user=self.DB_USER,
            password=self.DB_PASSWORD,
            host=self.DB_HOST,
            port=self.DB_PORT
        )
        self.cursor = self.conn.cursor()

        # Check if table named raw_data exists
        self.cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'raw_data')")
        if self.cursor.fetchone()[0]:
            logging.info("👾 TimescaleDB table 'raw_data' exists.")
        else:
            logging.info("👾 TimescaleDB table 'raw_data' does not exist. Creating it...")
            self.cursor.execute("""
                CREATE TABLE raw_data (
                    id SERIAL PRIMARY KEY,
                    timestamp BIGINT NOT NULL,
                    datetime TIMESTAMP NOT NULL,
                    device_id VARCHAR(50) NOT NULL,
                    datapoint VARCHAR(50) NOT NULL,
                    value TEXT NOT NULL,
                    did INTEGER
                )
            """)
            self.conn.commit()
            logging.info("👾 TimescaleDB table 'raw_data' created.")

    def init_rabbitmq_connection(self) -> None:
        """
        Initializes the RabbitMQ connection and sets up the exchange channel.
        """
        # RabbitMQ connection
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.RABBITMQ_HOST))
        self.channel = self.connection.channel()
        self.exchange = self.channel.exchange_declare(exchange=self.ALERT_EXCHANGE_NAME, exchange_type='topic')


    def insert_row_data(self, data) -> None:
        """
        Inserts processed row data into the raw_data database table.
        """
        insert_queries = []
        skipped_keys = ['datetime', 'device_id', 'id', 'sensor_type']
        timestamp = data["datetime"]
        
        for key in data.keys():
            if key in skipped_keys:
                continue
            insert_queries.append((
                int(datetime.fromisoformat(timestamp).timestamp()),
                datetime.fromisoformat(timestamp),
                data["device_id"],
                key,
                str(data[key]),
                data["id"]
            ))

        self.cursor.executemany(
            """
            INSERT INTO raw_data (timestamp, datetime, device_id, datapoint, value, did)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            insert_queries
        )
        self.conn.commit()

    def process_and_insert_data(self, data: Dict[str, Any]) -> None:
        """
        Processes incoming data and inserts it into the database if valid.
        """
        if not data.get("datetime"):
            logging.info(" [!] No timestamp found in data.")
            return
        try:
            self.insert_row_data(data)
        except (TypeError, ValueError):
            logging.error(" [!] Invalid IAQ sensor data received. Non-numeric values detected.")
            return
        except Exception as e:
            logging.error(f" [!] Error inserting data into TimescaleDB: {e}")
            return

    def get_default_thresholds(self) -> dict:
        """Retrieve the default thresholds."""
        return {
            'iaq': {
                'temperature_low': 18,
                'temperature_high': 26,
                'humidity_low': 30,
                'humidity_high': 60,
                'co2_low': 400,
                'co2_high': 1000
            },
            'power': {
                'power_spike_threshold': 1
            },
            'occupancy': {
                'stuck_occupied_timeout': 24
            }
        }

    def get_initial_thresholds(self) -> dict:
        """
        Retrieve initial fault detection thresholds from Supabase or use defaults.
        """
        response = self.supabase.table('fault_thresholds').select('*').execute()
        if response.data:
            logging.info("👾 Read thresholds from Supabase")
            return self.process_threshold_data(response.data[0])
        else:
            return self.get_default_thresholds()

    def process_threshold_data(self, record: dict) -> dict:
        """
        Processes threshold data from a record dictionary into a structured format.
        """
        return {
            'iaq': {
                'temperature_low': record.get('temperature_min'),
                'temperature_high': record.get('temperature_max'),
                'humidity_low': record.get('humidity_min'),
                'humidity_high': record.get('humidity_max'),
                'co2_low': record.get('co2_min'),
                'co2_high': record.get('co2_max')
            },
            'power': {
                'power_spike_threshold': record.get('power_kw_max')
            },
            'occupancy': {
                'stuck_occupied_timeout': record.get('sensitivity_max')
            }
        }

    async def setup_realtime_subscription(self) -> None:
        """
        Sets up a realtime subscription to monitor and handle table changes.
        """
        async_client = await create_async_client(config('SUPABASE_URL'), config('SUPABASE_API_KEY'))

        def handle_change(payload):
            logging.info("👾 Threshold updated from somewhere else!")
            if payload['event_type'] in ('INSERT', 'UPDATE'):
                self.fault_thresholds = self.process_threshold_data(payload['new'])
            elif payload['event_type'] == 'DELETE':
                self.fault_thresholds = self.get_default_thresholds()

        channel = await async_client.channel('threshold_changes')\
            .on_postgres_changes(
                event='UPDATE',
                schema='public',
                table='fault_thresholds',
                callback=handle_change
            ).subscribe()
        logging.info("👾[✓] Realtime subscription active for threshold values.", async_client.realtime.is_connected)
        while True:
            await asyncio.sleep(3600)

    def get_queues_for_exchange(self) -> list:
        """
        Retrieve the list of queue names bound to a specific RabbitMQ exchange.
        """
        rabbitmq_host = config('RABBITMQ_HOST')
        url = f'http://{rabbitmq_host}:15672/api/bindings'
        response = requests.get(url, auth=HTTPBasicAuth('guest', 'guest'))
        
        if response.status_code == 200:
            bindings = response.json()
            queues = [
                binding['destination']
                for binding in bindings
                if binding['source'] == config('EXCHANGE_NAME') and binding['destination_type'] == 'queue'
            ]
            return queues
        else:
            logging.error(f"Failed to fetch bindings: {response.status_code}")
            return []
    
    def run_fault_detection(self) -> None:
        """
        Listens for sensor data messages and performs fault detection.
        """
        channel = self.connection.channel()
        channel.exchange_declare(exchange=self.EXCHANGE_NAME, exchange_type='topic', durable=True)

        result = channel.queue_declare(queue='', durable=True, exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange=self.EXCHANGE_NAME, queue=queue_name, routing_key='#')

        logging.info("👾🐇 Waiting for sensor data...")

        def callback(ch, method, properties, body):
            try:
                data = json.loads(body.decode())
                self.detect_faults(data, method.routing_key)
            except Exception as e:
                logging.error("[!] Error processing message:", e)
        channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
        channel.start_consuming()

    def detect_faults(self, sensor_data, queue_name) -> None:
        """
        Detects faults in sensor data and publishes alerts if thresholds are exceeded.
        Args:
            sensor_data (dict): A dictionary containing sensor data. Expected keys include:
                - 'id' (str): The unique identifier for the data.
                - 'device_id' (str): The unique identifier for the device.
                - 'sensor_type' (str): The type of sensor (e.g., 'temperature', 'humidity', 'co2', 'power_meter', etc.).
                - 'datetime' (str): The timestamp of the sensor data in ISO 8601 format.
                - Additional keys depending on the sensor type (e.g., 'temperature', 'humidity', 'co2', 'power_meter', 'occupancy').
            queue_name (str): The name of the queue to which alerts should be published.
        Returns:
            None
        Behavior:
            - For 'temperature', 'humidity', and 'co2' sensors:
                - Checks if the values exceed predefined thresholds for indoor air quality (IAQ).
                - Publishes alerts for high or low temperature, humidity, or high CO2 levels.
                - Processes and inserts the sensor data, marking whether an IAQ fault was detected.
            - For 'power_meter' sensors:
                - Detects power spikes based on a predefined threshold.
                - Publishes alerts for power spikes.
                - Processes and inserts the sensor data, marking whether a power fault was detected.
            - For 'online_status', 'occupancy_status', and 'sensitivity' sensors:
                - Tracks room occupancy states and timestamps.
                - Detects if a room has been marked as occupied for too long, indicating a potential sensor issue.
                - Publishes alerts for stuck occupancy states.
                - Processes and inserts the sensor data, marking whether an occupancy fault was detected.
            - Logs warnings if incomplete sensor data is received.
        """
        did = sensor_data.get('id')
        device_id = sensor_data.get('device_id')
        sensor_type = sensor_data.get('sensor_type')
        timestamp = sensor_data.get('datetime')

        if not all([device_id, timestamp]):
            logging.warning(" [!] Incomplete sensor data received.")
            return

        if sensor_type in ['temperature', 'humidity', 'co2']:
            iaq_fault = False
            temperature = sensor_data.get('temperature')
            humidity = sensor_data.get('humidity')
            co2 = sensor_data.get('co2')

            if temperature is not None:
                if temperature > self.fault_thresholds['iaq']['temperature_high']:
                    self.publish_alert(queue_name, device_id, did, 'temperature_high', f'Temperature {temperature}°C exceeds threshold {self.fault_thresholds["iaq"]["temperature_high"]}°C', timestamp)
                elif temperature < self.fault_thresholds['iaq']['temperature_low']:
                    self.publish_alert(queue_name, device_id, did, 'temperature_low', f'Temperature {temperature}°C is below threshold {self.fault_thresholds["iaq"]["temperature_low"]}°C', timestamp)
            if humidity is not None:
                if humidity > self.fault_thresholds['iaq']['humidity_high']:
                    self.publish_alert(queue_name, device_id, did, 'humidity_high', f'Humidity {humidity}% exceeds threshold {self.fault_thresholds["iaq"]["humidity_high"]}%', timestamp)
                elif humidity < self.fault_thresholds['iaq']['humidity_low']:
                    self.publish_alert(queue_name, device_id, did, 'humidity_low', f'Humidity {humidity}% is below threshold {self.fault_thresholds["iaq"]["humidity_low"]}%', timestamp)
            if co2 is not None:
                if co2 > self.fault_thresholds['iaq']['co2_high']:
                    self.publish_alert(queue_name, device_id, did, 'co2_high', f'CO2 level {co2} ppm exceeds threshold {self.fault_thresholds["iaq"]["co2_high"]} ppm', timestamp)
            self.process_and_insert_data(sensor_data)

        elif sensor_type == 'power_meter':
            power_meter = sensor_data.get('power_meter')
            if power_meter is not None:
                # Simple power spike detection (can be enhanced with moving averages etc.)
                if power_meter > self.fault_thresholds['power']['power_spike_threshold']:
                    logging.info(f"👾💥 Power spike detected: {power_meter} kW on device {device_id}")
                    self.publish_alert(queue_name, device_id, did, 'power_spike', f'Power consumption spiked to {power_meter} kW on device {device_id}', timestamp)
            self.process_and_insert_data(sensor_data)

        elif sensor_type in ['online_status', 'occupancy_status', 'sensitivity']:
            room_id = device_id.split('-')[2]
            occupancy_state = sensor_data.get('occupancy')
            if occupancy_state == 'occupied':
                self.last_occupied[room_id] = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')
            elif occupancy_state == 'unoccupied' and room_id in self.last_occupied:
                del self.last_occupied[room_id] # Reset if unoccupied

            # Detect if a room has been occupied for too long (potential sensor issue)
            if room_id in self.last_occupied:
                if (datetime.now() - self.last_occupied[room_id]).total_seconds() > self.fault_thresholds['occupancy']['stuck_occupied_timeout'] * 3600:
                    self.publish_alert(queue_name, device_id, did, 'stuck_occupied', f'Room has been occupied since {self.last_occupied[room_id]}', timestamp)
            self.process_and_insert_data(sensor_data)

    def publish_to_rabbitmq(self, queue_name, alert_data) -> None:
        """
        Publishes alert data to a RabbitMQ queue.

        This method declares a queue with the specified queue name, binds it to the
        alert exchange using the routing key, and publishes the alert data to the queue.

        Args:
            queue_name (str): The name of the queue to publish the alert data to.
            alert_data (dict): The alert data to be published. It will be serialized to JSON format.

        Raises:
            pika.exceptions.AMQPError: If there is an error during queue declaration, binding, or publishing.
        """
        routing_key = f'_{queue_name}'
        self.channel.queue_declare(queue=routing_key)
        self.channel.queue_bind(exchange=self.ALERT_EXCHANGE_NAME, queue=routing_key, routing_key=routing_key)
        self.channel.basic_publish(exchange=self.ALERT_EXCHANGE_NAME, routing_key=routing_key, body=json.dumps(alert_data))

    def publish_to_supabase(self, alert_data) -> None:
        """
        Publishes alert data to the Supabase table.

        Args:
            alert_data (dict): A dictionary containing the alert data to be published.

        Returns:
            None
        """
        try:
            response = self.supabase.table('fault_status').insert(alert_data).execute()
        except Exception as e:
            logging.error(f" [!] Error publishing to Supabase: {e}")
            return

    def publish_alert(self,  queue_name, device_id, did, fault_type, message, timestamp) -> None:
        """
        Publishes an alert to RabbitMQ and Supabase.

        This method sends an alert message containing fault details to a specified
        RabbitMQ queue and also stores the alert data in Supabase for further processing
        or record-keeping.

        Args:
            queue_name (str): The name of the RabbitMQ queue to publish the alert to.
            device_id (str): The unique identifier of the device where the fault occurred.
            did (str): The unique identifier for the detection instance.
            fault_type (str): The type of fault detected (e.g., "temperature_fault").
            message (str): A descriptive message providing details about the fault.
            timestamp (str): The timestamp when the fault was detected.

        Returns:
            None
        """
        alert_data = {
            'device_id': device_id,
            'fault_type': fault_type,
            'status': 'open',
            'message': message,
            'detected_at': timestamp,
            'did': did,
        }
        self.publish_to_rabbitmq(queue_name, alert_data)
        self.publish_to_supabase(alert_data)

async def main():
    try:
        logging.info()
        logging.info("👾 Initializing Fault Detection Agent...")
        agent = FaultDetectionAgent()
        agent.init_db_connection()
        agent.init_rabbitmq_connection()
        agent.fault_thresholds = agent.get_initial_thresholds()
        logging.info("👾🔧 Initial Thresholds loaded")

        thread = Thread(target=agent.run_fault_detection, daemon=True)
        thread.start()

        await agent.setup_realtime_subscription()
    except KeyboardInterrupt:
        logging.info(" [!] Exiting...")
    except Exception as e:
        logging.error(f" [!] Error: {e}")
    finally:
        logging.info(" [✓] Connections closed.")

if __name__ == '__main__':
    main()
