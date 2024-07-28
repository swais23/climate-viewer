# Use a base image with Python
FROM python:3.8-slim AS base

# Install packages
COPY requirements/*.txt /req/
RUN cd /req; pip install pip-tools; pip install -r requirements.txt

# Set working directory
WORKDIR /src
