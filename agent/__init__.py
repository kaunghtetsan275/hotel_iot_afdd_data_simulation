""" Simulation agent.

    A simulator for Indoor Air Quality (IAQ) sensors that generates and publishes simulated sensor data 
    to RabbitMQ and Supabase. This class supports advanced data generation for temperature, humidity, 
    CO2 levels, occupancy, and power consumption, and provides methods for publishing data to RabbitMQ 
    and Supabase.

    Fault Detection Agent 
     
    Designed to facilitate fault detection in IoT-based hotel systems. 
    It integrates with various services such as RabbitMQ, Supabase, and TimescaleDB to process sensor 
    data, detect anomalies, and publish alerts. The class provides methods for initializing database 
    connections, managing RabbitMQ exchanges and queues, subscribing to real-time updates, and 
    detecting faults based on predefined or dynamically updated thresholds.
    
    Key Features:
    - Connects to TimescaleDB for storing and querying sensor data.
    - Integrates with RabbitMQ for message consumption and alert publishing.
    - Uses Supabase for managing fault thresholds and storing fault status.
    - Supports real-time subscription to threshold updates via Supabase.
    - Detects faults in indoor air quality (IAQ), power consumption, and occupancy sensors.
    - Publishes alerts to RabbitMQ and Supabase when faults are detected.
    - Provides default and customizable fault thresholds for various sensor types.
    This class is designed to run continuously, consuming sensor data, detecting faults, and 
    publishing alerts in real-time, making it suitable for IoT-based fault detection systems.
"""
__version__ = "1.0.0"
__author__ = "Kaung Htet San :: Kevin"

from .agent_simulation import IAQSensorSimulator
from .agent_fault_detection import FaultDetectionAgent
__all__ = ['IAQSensorSimulator', 'FaultDetectionAgent'] 