import asyncio
import math
import json
import random
from decouple import config
from datetime import datetime, timedelta
from supabase import create_client
from aio_pika import connect_robust, Message
import logging

class IAQSensorSimulator:
    """
    Attributes:
        RABBITMQ_URL (str): The RabbitMQ connection URL.
        RABBITMQ_HOST (str): The RabbitMQ host address.
        EXCHANGE_NAME (str): The name of the RabbitMQ exchange.
        interval_seconds (int): The interval in seconds between data generation cycles.
        interval_total_seconds (int): The total elapsed time in seconds for the simulation.
        connection (aio_pika.Connection): The RabbitMQ connection object.
        channel (aio_pika.Channel): The RabbitMQ channel object.
        exchange (aio_pika.Exchange): The RabbitMQ exchange object.
        supabase (supabase.Client): The Supabase client for interacting with the database.
    Methods:
        __init__():
            Initializes the IAQSensorSimulator with RabbitMQ and Supabase configurations.
        init_rabbitmq():
            Asynchronously initializes the RabbitMQ connection, channel, and exchange.
        close_rabbitmq():
            Asynchronously closes the RabbitMQ connection and channel.
        get_devices_from_supabase():
            Fetches the list of devices from the Supabase database.
        generate_iaq_data(device_id):
            Generates basic IAQ data (temperature, humidity, CO2) for a given device.
        generate_iaq_data_advanced(device_id, did, sensor_type, ...):
            Generates advanced IAQ data with sinusoidal patterns, noise, and spikes.
        generate_occupancy_data_advanced(device_id, did, sensor_type):
            Generates advanced occupancy data, including online status, sensitivity, and occupancy status.
        generate_power_data_advanced(device_id, did, sensor_type):
            Generates advanced power consumption data with base levels, noise, and spikes.
        generate_iot_data(device_id, did, sensor_type):
            Generates IoT data for a specific sensor type (e.g., temperature, humidity, power meter).
        publish_to_rabbitmq(data_list):
            Publishes a list of sensor data to RabbitMQ with appropriate routing keys.
        publish_batch(json_list):
            Asynchronously publishes a batch of sensor data to RabbitMQ.
        publish_to_supabase(data_list):
            Publishes a list of sensor data to the Supabase database.
        run_simulation():
            Runs the IAQ sensor simulation, generating and publishing data at regular intervals.
    """
    def __init__(self):
        """
        Initializes the simulation agent with RabbitMQ and Supabase configurations.
        """
        # RabbitMQ connection parameters
        self.RABBITMQ_URL = 'amqp://guest:guest@localhost:5672/' if config('ENVIRONMENT', default='local') == 'local' else config('RABBITMQ_URL')
        self.RABBITMQ_HOST = 'localhost' if config('ENVIRONMENT', default='local') == 'local' else config('RABBITMQ_HOST')
        self.EXCHANGE_NAME = config('EXCHANGE_NAME', default='hotel_iot')
        self.interval_seconds = 5
        self.interval_total_seconds = self.interval_seconds

        # RabbitMQ setup
        self.connection = None
        self.channel = None
        self.exchange = None

        # Supabase setup
        # Initialize Supabase client
        SUPABASE_URL = config('SUPABASE_URL')
        SUPABASE_API_KEY = config('SUPABASE_API_KEY')
        self.supabase = create_client(SUPABASE_URL, SUPABASE_API_KEY)

    async def init_rabbitmq(self) -> None:
        """
        Initialize a connection to RabbitMQ using a robust connection and set up
        a communication channel with a durable topic exchange.

        This method establishes a robust connection to the RabbitMQ server using
        the provided RabbitMQ URL, creates a communication channel, and declares
        a durable topic exchange with the specified exchange name.
        """
        self.connection = await connect_robust(self.RABBITMQ_URL)
        self.channel = await self.connection.channel()
        self.exchange = await self.channel.declare_exchange(
                            name=self.EXCHANGE_NAME,
                            type='topic',
                            durable=True
                        )

    async def close_rabbitmq(self) -> None:
        """
        Asynchronously closes the RabbitMQ channel and connection.

        This method ensures that both the channel and connection to RabbitMQ
        are properly closed if they exist. It first closes the channel, followed
        by the connection, to release resources and clean up.

        Raises:
            Any exceptions raised during the closing of the channel or connection.
        """
        if self.channel:
            await self.channel.close()
        if self.connection:
            await self.connection.close()

    def get_devices_from_supabase(self) -> list:
        """
        Fetches a list of devices from the Supabase database.

        This method queries the 'devices' table in the Supabase database and retrieves
        all records. If devices are found, they are returned as a list. If no devices
        are found, an empty list is returned, and a message is printed to indicate this.

        Returns:
            list: A list of device records if found, otherwise an empty list.
        """
        # Fetch devices from Supabase
        devices = self.supabase.table('devices').select('*').execute()
        if devices.data:
            return devices.data
        else:
            logging.info("No devices found in Supabase.")
            return []

    def generate_iaq_data_advanced(self, device_id, did, sensor_type,
                                   start_time_str="2024-12-27 00:00:00",
                                   temperature_base=27,
                                   temperature_amplitude=0.5,
                                   humidity_base=50,
                                   humidity_amplitude=0.5,
                                   co2_base=500,
                                   co2_spike_chance=0.01,
                                   co2_spike_magnitude_range=(300, 800),
                                   temperature_noise_std=0.05,
                                   humidity_noise_std=1.5,
                                   co2_noise_std=20) -> tuple:
        """
        Generate advanced Indoor Air Quality (IAQ) data including temperature, humidity, and CO2 levels.
        This method simulates IAQ data using sinusoidal patterns, diurnal adjustments, random noise, 
        and occasional CO2 spikes to mimic real-world environmental variations.
        Args:
            device_id (str): The unique identifier for the device generating the data.
            did (str): Device ID or additional identifier for the data source.
            sensor_type (str): The type of sensor generating the data.
            start_time_str (str, optional): The starting timestamp for the simulation in the format 
                "YYYY-MM-DD HH:MM:SS". Defaults to "2024-12-27 00:00:00".
            temperature_base (float, optional): The base temperature value in degrees Celsius. Defaults to 27.
            temperature_amplitude (float, optional): The amplitude of the sinusoidal temperature variation. Defaults to 0.5.
            humidity_base (float, optional): The base humidity value in percentage. Defaults to 50.
            humidity_amplitude (float, optional): The amplitude of the sinusoidal humidity variation. Defaults to 0.5.
            co2_base (int, optional): The base CO2 level in ppm. Defaults to 500.
            co2_spike_chance (float, optional): The probability of a CO2 spike occurring at each time step. Defaults to 0.01.
            co2_spike_magnitude_range (tuple, optional): The range of CO2 spike magnitudes in ppm. Defaults to (300, 800).
            temperature_noise_std (float, optional): The standard deviation of random noise added to temperature. Defaults to 0.05.
            humidity_noise_std (float, optional): The standard deviation of random noise added to humidity. Defaults to 1.5.
            co2_noise_std (float, optional): The standard deviation of random noise added to CO2 levels. Defaults to 20.
        Returns:
            tuple: A tuple containing:
                - temperature (float): The simulated temperature value in degrees Celsius.
                - humidity (float): The simulated humidity value in percentage.
                - co2 (float): The simulated CO2 level in ppm.
        """
        start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
        current_time = start_time + self.interval_total_seconds * timedelta(seconds=1)
        t_sec = current_time.second
        co2_spike_value = 0
        co2_spike_decay = 0

        # Temperature: base + sinusoidal pattern with a 24-hour cycle + noise
        temperature = temperature_base + temperature_amplitude * math.sin(2 * math.pi * t_sec / 86400 - math.pi / 2)
        hour_of_day = current_time.hour + current_time.minute / 60
        diurnal_adjustment = -5 * math.cos(2 * math.pi * hour_of_day / 24)
        temperature += diurnal_adjustment
        temperature += random.gauss(0, temperature_noise_std)

        # Humidity inverse of temperature with sinusoidal pattern + noise
        humidity = humidity_base - humidity_amplitude * math.sin(2 * math.pi * t_sec / 86400 - math.pi / 2)
        humidity += random.gauss(0, humidity_noise_std)
        humidity = max(30, min(70, humidity))

        # CO2 base level and spikes
        co2_base = max(400, min(1200, co2_base))
        if co2_spike_value > 0:
            co2_spike_value *= co2_spike_decay
        elif random.random() < co2_spike_chance:
            co2_spike_value = random.randint(*co2_spike_magnitude_range)
            co2_spike_decay = random.uniform(0.95, 0.99)
        co2 = co2_base + co2_spike_value + random.gauss(0, co2_noise_std)

        return temperature, humidity, co2

    def generate_occupancy_data_advanced(self, device_id, did, sensor_type) -> tuple:
        """
        Generates advanced occupancy data for a given device.
        This method simulates the occupancy status, online status, and sensitivity 
        of a device based on the current time and random chance. It also introduces 
        a small probability of anomalies in the generated data.
        Args:
            device_id (str): The unique identifier of the device.
            did (str): The device ID used for internal processing.
            sensor_type (str): The type of sensor associated with the device.
        Returns:
            tuple: A tuple containing:
                - online_status (str): The online status of the device, either 'online' or 'offline'.
                - sensitivity (str): The sensitivity level of the device, represented as a percentage 
                    ('0%', '25%', '50%', '75%', or '100%').
                - occupancy_status (str): The occupancy status of the device, either 'occupied' or 'unoccupied'.
        """
        current_time = datetime.strptime("2024-12-27 00:00:00", "%Y-%m-%d %H:%M:%S")
        current_time = current_time + self.interval_total_seconds * timedelta(seconds=1)
        if 0 <= current_time.hour < 6 or 12 <= current_time.hour < 18:
            occupancy_status = 'unoccupied'
            occupancy_chance = random.random()
            if occupancy_chance < 0.02:
                occupancy_status = 'occupied'
        else:
            occupancy_status = 'occupied'

        online_chance = random.random()
        if online_chance < 0.02:
            online_status = 'offline'
        else:
            online_status = 'online'

        if online_status == 'offline':
            sensitivity = '0%'
        elif random.random() > 0.02:
            sensitivity = '100%'
        else:
            sensitivity = random.choice(['0%', '25%', '50%', '75%'])
        
        return online_status, sensitivity, occupancy_status

    def generate_power_data_advanced(self, device_id, did, sensor_type) -> float:
        """
        Generates simulated power consumption data for a device with advanced features 
        such as random noise, base power, and occasional power spikes.
        Args:
            device_id (str): The unique identifier for the device.
            did (str): Additional identifier for the device (not used in the current implementation).
            sensor_type (str): The type of sensor associated with the device (not used in the current implementation).
        Returns:
            float: Simulated power consumption value in kilowatts (kW).
        Notes:
            - The base power consumption is randomly generated within a range of 0.1 to 2.5 kW.
            - Power spikes occur with a 10% probability and have a magnitude between 3 and 10 kW.
            - Power spikes decay over time with a random decay factor between 0.95 and 0.99.
            - Random Gaussian noise with a standard deviation of 0.1 kW is added to the final power value.
        """
        power_spike_magnitude_range = (3, 10)  # kW
        power_base = random.uniform(0.1, 2.5)  # kW
        power_noise_std = 0.1  # kW
        power_spike_chance = random.random()
        power_spike_decay = random.uniform(0.95, 0.99)
        power_spike_value = 0

        if power_spike_value > 0:
            power_spike_value *= power_spike_decay
        elif power_spike_chance < 0.1:  # 10% chance of a spike
            power_spike_value = random.randint(*power_spike_magnitude_range)
            power_spike_decay = random.uniform(0.95, 0.99)

        power_meter = power_base + power_spike_value + random.gauss(0, power_noise_std)
        return power_meter

    def generate_iot_data(self, device_id, did, sensor_type) -> dict:
        """
        Generates IoT data for a given device based on the specified sensor type.
        Args:
            device_id (str): The unique identifier of the IoT device.
            did (str): The unique identifier for the data entry.
            sensor_type (str): The type of sensor data to generate. 
                Supported values are:
                - 'power_meter': Generates power meter data.
                - 'temperature': Generates temperature data.
                - 'humidity': Generates humidity data.
                - 'co2': Generates CO2 data.
                - 'online_status': Generates online status data.
                - 'occupancy_status': Generates occupancy status data.
                - 'sensitivity': Generates sensitivity data.
        Returns:
            dict: A dictionary containing the generated IoT data with the following keys:
                - "datetime" (str): The ISO 8601 formatted timestamp of the data.
                - "id" (str): The unique identifier for the data entry.
                - "device_id" (str): The unique identifier of the IoT device.
                - "<sensor_type>" (varies): The generated value for the specified sensor type.
                - "sensor_type" (str): The type of sensor data generated.
        Notes:
            - The method uses helper functions to generate specific types of data:
              `generate_iaq_data_advanced`, `generate_occupancy_data_advanced`, and 
              `generate_power_data_advanced`.
            - The `locals()` function is used to dynamically access the generated 
              sensor data based on the `sensor_type`.
        """
        current_time = datetime.strptime("2024-12-27 00:00:00", "%Y-%m-%d %H:%M:%S")
        current_time = current_time + self.interval_total_seconds * timedelta(seconds=1)

        # NOTE: These variables are used in locals() to access the generated data
        temperature, humidity, co2 = self.generate_iaq_data_advanced(device_id, did, sensor_type)
        online_status, sensitivity, occupancy_status = self.generate_occupancy_data_advanced(device_id, did, sensor_type)
        power_meter = self.generate_power_data_advanced(device_id, did, sensor_type)
        if sensor_type == 'power_meter':
            return {
                "datetime": current_time.isoformat(),
                'id': did,
                "device_id": device_id,
                "power_meter": round(power_meter, 2),
                "sensor_type": sensor_type
            }
        elif sensor_type in ['temperature', 'humidity', 'co2']:
            return {
                "datetime": current_time.isoformat(),
                "id": did,
                "device_id": device_id,
                sensor_type: round(locals()[sensor_type], 2),
                "sensor_type": sensor_type
            }
        elif sensor_type in ['online_status', 'occupancy_status', 'sensitivity']:
            return {
                "datetime": current_time.isoformat(),
                "id": did,
                "device_id": device_id,
                sensor_type: locals()[sensor_type],
                "sensor_type": sensor_type
            }

    def publish_to_rabbitmq(self, data_list) -> None:
        """
        Publishes a list of data to RabbitMQ with dynamically generated routing keys.
        This method processes a list of data dictionaries, determines the appropriate
        routing key for each data item based on its `sensor_type` and `device_id`, and
        publishes the data to a RabbitMQ exchange.
        Args:
            data_list (list): A list of dictionaries, where each dictionary represents
                a data item to be published. Each dictionary must contain the following keys:
                - 'device_id' (str): The unique identifier of the device, formatted as a
                  string with parts separated by hyphens.
                - 'sensor_type' (str): The type of sensor (e.g., 'temperature', 'humidity',
                  'power_meter_...', etc.).
        Routing Key Generation:
            - For sensor types starting with 'power_meter_', the routing key suffix is "POW".
            - For other sensor types, the suffix is determined by the `routing_key_map`.
            - If the sensor type is not found in the `routing_key_map`, the suffix defaults
              to "UNKNOWN".
            - The final routing key is constructed by combining the first three parts of
              the `device_id` with the suffix, separated by hyphens.
        RabbitMQ Operations:
            - Declares a queue with the generated routing key.
            - Binds the queue to the specified exchange using the routing key.
            - Publishes the data to the exchange with the routing key.
        Raises:
            KeyError: If a required key ('device_id' or 'sensor_type') is missing from a
                data dictionary in the `data_list`.
            Exception: If there is an issue with RabbitMQ operations (e.g., queue declaration,
                binding, or publishing).
        Note:
            This method assumes that `self.channel` is a valid RabbitMQ channel and
            `self.EXCHANGE_NAME` is the name of the RabbitMQ exchange to publish to.
        """
        routing_key_map = {
            'temperature': 'IAQ',
            'humidity': 'IAQ',
            'co2': 'IAQ',
            'online_status': 'OCC',
            'occupancy_status': 'OCC',
            'sensitivity': 'OCC'
        }

        for data in data_list:
            device_id_parts = data['device_id'].split('-')
            sensor_type = data['sensor_type']
            if sensor_type.startswith('power_meter_'):
                routing_key_suffix = "POW"
            else:
                routing_key_suffix = routing_key_map.get(sensor_type, "UNKNOWN")
            routing_key = '-'.join(device_id_parts[:3]) + f"-{routing_key_suffix}"
            self.channel.queue_declare(queue=routing_key)
            self.channel.queue_bind(exchange=self.EXCHANGE_NAME, queue=routing_key, routing_key=routing_key)
            self.channel.basic_publish(exchange=self.EXCHANGE_NAME, routing_key=routing_key, body=json.dumps(data))

    async def publish_batch(self, json_list) -> None:
        """
        Publishes a batch of JSON messages to RabbitMQ queues based on sensor types.
        This asynchronous method connects to a RabbitMQ server, declares queues dynamically
        based on the `sensor_type` field in each message, binds the queues to an exchange,
        and publishes the messages to the appropriate queues.
        Args:
            json_list (list): A list of dictionaries, where each dictionary represents a 
                              JSON message containing a `sensor_type` key and other data.
        Raises:
            Exception: If there is an issue with connecting to RabbitMQ, declaring queues,
                       or publishing messages.
        Notes:
            - The RabbitMQ connection URL is retrieved from the `RABBITMQ_URL` environment variable.
            - The queues are declared as durable, ensuring they persist even if RabbitMQ restarts.
            - The messages are published with a delivery mode of 2, making them persistent.
            - The method uses asyncio to handle multiple tasks concurrently.
        """
        connection = await connect_robust(config('RABBITMQ_URL'))
        channel = await connection.channel()
        tasks = []
        for msg in json_list:
            sensor_type = msg['sensor_type']
            queue_name = f"{sensor_type}_queue"
            await channel.declare_queue(queue_name, durable=True)
            queue = await channel.declare_queue(queue_name, durable=True)
            await queue.bind(self.EXCHANGE_NAME, routing_key=queue_name)
            tasks.append(
                self.exchange.publish(
                    Message(body=json.dumps(msg).encode(), delivery_mode=2),
                    routing_key=queue_name
                )
            )

        await asyncio.gather(*tasks)
        await connection.close()

    def publish_to_supabase(self, data_list) -> None:
        """
        Publishes a list of sensor data to a Supabase table.

        This method transforms the input data into the required format and uploads it
        to the 'sensor_data_latest' table in Supabase. The transformation includes
        mapping the input data to a dictionary with specific keys and converting the
        timestamp to a Unix timestamp.

        Args:
            data_list (list): A list of dictionaries, where each dictionary contains
                the following keys:
                - 'device_id' (str): The unique identifier of the device.
                - 'sensor_type' (str): The type of sensor (e.g., temperature, humidity).
                - 'datetime' (str): The ISO 8601 formatted datetime string.
                - 'id' (int): The unique identifier for the data point.
                - A key matching the value of 'sensor_type' (e.g., 'temperature'): The
                  sensor reading value.

        Raises:
            ValueError: If the 'datetime' field in the input data cannot be parsed
                into a valid datetime object.

        Notes:
            - The 'timestamp' field is derived from the 'datetime' field and is stored
              as a Unix timestamp.
            - The 'did' field is derived from the 'id' field and is stored as an integer.
            - The 'upsert' operation ensures that existing records with the same
              'device_id' and 'datapoint' are updated, while new records are inserted.
        """
        # transform data_list to a list of dictionaries with the required keys to match the Supabase table
        data_list = list(map(lambda x: {
            'device_id': x['device_id'],
            'datapoint': x['sensor_type'],
            'value': x[x['sensor_type']],
            'timestamp': int(datetime.fromisoformat(x['datetime']).timestamp()),
            'datetime': x['datetime'],
            'did': int(x['id'])}, data_list))
        self.supabase.table('sensor_data_latest').upsert(data_list, on_conflict='device_id, datapoint').execute()

    async def run_simulation(self) -> None:
        """
        Runs the IAQ (Indoor Air Quality) Sensor Simulator.
        This asynchronous method initializes RabbitMQ, retrieves devices from Supabase, 
        generates IoT data for each device, and publishes the data to both Supabase 
        and RabbitMQ in batches. The simulation runs for a specified number of entries 
        or until interrupted.
        """
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        # Silence Supabase Realtime logs
        logging.getLogger("pika").setLevel(logging.WARNING)
        logging.getLogger('realtime').setLevel(logging.WARNING)
        logging.getLogger('realtime._async.client').setLevel(logging.WARNING)
        logging.getLogger('realtime._async.channel').setLevel(logging.WARNING)
        # Optionally, silence other related loggers
        logging.getLogger('httpx').setLevel(logging.WARNING)
        logging.getLogger('asyncio').setLevel(logging.WARNING)
        logging.getLogger('phx_websocket').setLevel(logging.WARNING)
        logging.getLogger('phoenix').setLevel(logging.WARNING)
        logging.getLogger('websockets').setLevel(logging.WARNING)

        logging.info("🤖 IAQ Sensor Simulator started.")
        logging.info("🤖 RabbitMQ dashboard available at: http://localhost:15672")
        get_devices = self.get_devices_from_supabase()
        await self.init_rabbitmq()

        try:
            max_entries = config('MAX_ENTRIES', default=1000, cast=int)
            for _ in range(max_entries):
                iaq_data_list = list(map(
                    lambda d: self.generate_iot_data(d['device_identifier'], d['id'], d['sensor_type']),
                    get_devices
                ))
                iaq_data_list = list(filter(None, iaq_data_list))
                self.publish_to_supabase(iaq_data_list)
                await self.publish_batch(iaq_data_list)

                self.interval_total_seconds += self.interval_seconds
                await asyncio.sleep(self.interval_seconds)

        except KeyboardInterrupt:
            logging.info("IAQ Sensor Simulator stopped.")
        except Exception as e:
            logging.error(f"Simulation error: {e}")
        finally:
            await self.close_rabbitmq()
            logging.info("Cleanup done. Bye!")

async def main():
    """
    Entry point for the simulation script.

    This function initializes an instance of the IAQSensorSimulator class
    and runs the simulation asynchronously using asyncio.
    """
    simulator = IAQSensorSimulator()
    await simulator.run_simulation()

if __name__ == '__main__':
    main()
