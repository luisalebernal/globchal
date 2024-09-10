# Use a more recent version of Python
# Use a more recent and compatible version of Python based on Debian
FROM python:3.10-slim-buster
# Install build dependencies
RUN apt-get update && \
    apt-get install -y \
    build-essential \
    g++ \
    cmake \
    libffi-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and setuptools
RUN pip install --upgrade pip setuptools

# Define the present working directory
WORKDIR /Data-Engineer-Challenge

# Copy the contents into the working directory
COPY . /Data-Engineer-Challenge

# Install the dependencies
RUN pip install -r requirements.txt

# Expose port 5000 for the Flask application
EXPOSE 3000

# Define the command to run the Flask application using Gunicorn
CMD ["python3","main_flask.py"]

