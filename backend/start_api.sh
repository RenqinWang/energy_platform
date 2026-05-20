#!/bin/bash

# Start Energy Platform API Server
# This script sets up the environment and starts the FastAPI backend

# Set environment variables
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_HOME=/home/student/spark-3.5.7-bin-hadoop3
export PATH=$SPARK_HOME/bin:$PATH

# Navigate to backend directory
cd /home/student/energy-platform/backend

# Activate conda base environment
eval "$(conda shell.bash hook)"
conda activate base

# Check if dependencies are installed
if ! python -c "import fastapi, uvicorn, pyspark" 2>/dev/null; then
    echo "Installing dependencies..."
    pip install -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt || \
    pip install -r requirements.txt
fi

# Start the API server
echo "Starting Energy Platform API server on http://0.0.0.0:8000"
echo "API documentation available at http://0.0.0.0:8000/docs"
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
