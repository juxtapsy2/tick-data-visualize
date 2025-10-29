#!/bin/sh
set -e

# Use PORT environment variable from Render, default to 80 for local
PORT=${PORT:-80}

echo "Starting services on port $PORT"

# Substitute PORT in nginx config
sed -i "s/listen 80;/listen $PORT;/g" /etc/nginx/http.d/default.conf

# Start supervisord
exec /usr/bin/supervisord -c /etc/supervisord.conf
