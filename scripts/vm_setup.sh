#!/bin/bash
set -e

echo "=== Step 1: Update system ==="
sudo apt-get update -y

echo "=== Step 2: Install Docker ==="
sudo apt-get install -y ca-certificates curl gnupg lsb-release
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update -y
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

echo "=== Step 3: Add user to docker group ==="
sudo usermod -aG docker $USER

echo "=== Step 4: Install Python 3.11 + git ==="
sudo apt-get install -y python3.11 python3.11-venv python3-pip git

echo "=== Step 5: Verify ==="
docker --version
docker compose version
python3.11 --version
git --version

echo "=== VM setup complete ==="
