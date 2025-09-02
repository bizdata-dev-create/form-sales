#!/bin/bash

# Set system-wide locale
sudo update-locale LANG=ja_JP.UTF-8 LC_ALL=ja_JP.UTF-8

# Add to /etc/environment
echo 'export LANG=ja_JP.UTF-8' | sudo tee -a /etc/environment
echo 'export LC_ALL=ja_JP.UTF-8' | sudo tee -a /etc/environment
echo 'export PYTHONIOENCODING=UTF-8' | sudo tee -a /etc/environment

# Add to sudoers
sudo bash -c 'echo "Defaults env_keep += \"LANG LC_ALL PYTHONIOENCODING\"" >> /etc/sudoers.d/locale'

# Set permissions
sudo chmod 440 /etc/sudoers.d/locale

echo "=== Locale setup completed ==="
echo "Current locale:"
locale

echo "=== Environment variables ==="
env | grep -E "(LANG|LC_ALL|PYTHON)"
