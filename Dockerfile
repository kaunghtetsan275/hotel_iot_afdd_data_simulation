# Use the official Python image as the base image
FROM python:3.13-alpine

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory in the container
WORKDIR /simulation

# Copy the requirements file into the container
COPY requirements.txt /simulation/requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the simulation project into the container
COPY . /simulation/

# Run the simulation
CMD ["python","run.py"]