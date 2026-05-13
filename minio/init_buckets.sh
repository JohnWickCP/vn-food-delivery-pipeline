#!/bin/bash
set -e

MC_ALIAS="local"
MINIO_URL="http://minio:9000"
BUCKET="food-delivery-lake"

echo "Waiting for MinIO to be ready..."
until mc alias set "$MC_ALIAS" "$MINIO_URL" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" 2>/dev/null; do
  sleep 2
done

echo "Creating bucket: $BUCKET"
mc mb --ignore-existing "$MC_ALIAS/$BUCKET"

# Create placeholder objects to establish Bronze layer prefix structure
for prefix in raw/orders raw/payments raw/rider_events; do
  echo "Initializing prefix: $BUCKET/$prefix"
  echo "" | mc pipe "$MC_ALIAS/$BUCKET/$prefix/.keep"
done

echo "MinIO init complete."
mc ls "$MC_ALIAS/$BUCKET"
