#!/bin/bash

KEY_PATH="/c/Users/c4dex/OneDrive/Desktop/m4-dept01/m4-dept01-spark.pem"          
EC2_USER="ubuntu"                     
EC2_IP="18.207.167.93"       
REMOTE_DIR="~/m4/spark/app"          

# echo "Subiendo carpeta ctes..."
scp -i $KEY_PATH -r app/ctes $EC2_USER@$EC2_IP:$REMOTE_DIR/

# echo "Subiendo carpeta utils..."
scp -i $KEY_PATH -r app/utils $EC2_USER@$EC2_IP:$REMOTE_DIR/

# echo "Subiendo weather_gold_job.py..."
scp -i $KEY_PATH app/weather_gold_job.py $EC2_USER@$EC2_IP:$REMOTE_DIR/

# echo "Subiendo weather_silver_job.py..."
scp -i $KEY_PATH app/weather_silver_job.py $EC2_USER@$EC2_IP:$REMOTE_DIR/

echo "[upload2ec2] --> OK"
