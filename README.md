# IoT Sensor Data Simulation And Fault Detection Agent
Sensor data simulation agent is for Indoor Air Quality (IAQ) sensors that generates and publishes simulated sensor data to RabbitMQ and Supabase. This agent supports advanced data generation for temperature, humidity, CO2 levels, occupancy, and power consumption, and provides methods for publishing data to RabbitMQ and Supabase.<br>

Fault detection agent is designed to facilitate fault detection in IoT-based hotel systems. It integrates with various services such as RabbitMQ, Supabase, and TimescaleDB to process sensor data, detect anomalies, and publish alerts. The class provides methods for initializing database connections, managing RabbitMQ exchanges and queues, subscribing to real-time updates, and detecting faults based on predefined or dynamically updated thresholds. <br>

## Sensor Data Pattern & Simulation Method
IAQ data simulation uses sinusoidal patterns, diurnal adjustments, random noise, and occasional CO2 spikes to mimic real-world environmental variations.
The temperature is determined based on:
- random probabilities,
- current time of day and
- daytime since temperature will fall in night and rise during day.

Simulation of Humidity is based on:
- inverse relation to temperature and sinusoidal patterns

Occasional CO2 spikes is generated to mimic real-world environmental variations with parameters such as
- spike magnitude
- spike decay

To simulate the presense status, online status, and sensitivity of a device, the method is based on the current time and random chance. Between 00:00 and 6:00, it will be unoccupied and again between 12:00 and 18:00. The room will be occupied at other times. It also introduces a small probability of anomalies in the generated data.

The online status is determined randomly. There is :-
- 2% chance of being "offline."
- 98% chance of being "online."

Sensitivity depends on the online status:
- If "offline," sensitivity is always '0%'.
- If "online," there is a 98% chance of '100%' sensitivity.
- Otherwise, sensitivity is randomly chosen from a set of values ('0%', '25%', '50%', '75%').

For power meter data simulation, base power consumption is randomly generated within a range of 0.1 to 2.5 kW.
- Power spikes occur with a 10% probability and have a magnitude between 3 and 10 kW.
- Power spikes decay over time with a random decay factor between 0.95 and 0.99.
- Random Gaussian noise with a standard deviation of 0.1 kW is added to the final power value.

