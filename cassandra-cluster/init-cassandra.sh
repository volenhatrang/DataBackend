#!/bin/bash

# Lấy IP của container hiện tại
NODE_IP=$(hostname -i)

if [ "$CASSANDRA_SEED" == "true" ]; then
  echo "This is a seed node with IP: $NODE_IP"
else
  echo "This is a regular node with IP: $NODE_IP, connecting to seed..."
fi

exec /docker-entrypoint.sh "$@"