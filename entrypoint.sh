#!/bin/sh
# entrypoint.sh — Launch the pipeline with configurable record count
# Usage: docker run pipeline [COUNT]  (default: 50000000)

COUNT=${1:-50000000}
BOOTSTRAP=${BOOTSTRAP:-kafka:9092}

echo "============================================"
echo " Kafka Pipeline v2 (Dockerized)"
echo "============================================"
echo "  Records:    $COUNT"
echo "  Bootstrap:  $BOOTSTRAP"
echo ""

exec java \
  -Xms256m -Xmx768m \
  -XX:+UseG1GC \
  -XX:MaxGCPauseMillis=200 \
  -XX:G1HeapRegionSize=16m \
  -XX:+UseStringDeduplication \
  -XX:ConcGCThreads=2 \
  -Dorg.slf4j.simpleLogger.defaultLogLevel=error \
  -jar app.jar \
  --count "$COUNT" \
  --bootstrap "$BOOTSTRAP" \
  --dataDir /app/data
