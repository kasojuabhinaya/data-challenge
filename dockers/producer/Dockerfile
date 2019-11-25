FROM python:3.7-slim-buster

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install the package
RUN pip3 install -r requirements.txt

# Run this when the container starts
CMD ["python3", "producer.py"]