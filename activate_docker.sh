#!/bin/bash

# Add current user to docker group
sudo usermod -aG docker "$USER"

echo "Starting a new shell session with docker group permissions..."
echo "When this shell opens, run: docker ps"
sleep 1

# Start a new shell session
exec newgrp docker