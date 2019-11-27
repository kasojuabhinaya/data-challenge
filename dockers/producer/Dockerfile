FROM python:3.7-slim-buster

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app
RUN chmod +x start.sh

# Install the package
RUN pip3 install -r requirements.txt

# Run this when the container starts
ENTRYPOINT ["sh", "start.sh"]
