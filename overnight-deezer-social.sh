#!/bin/bash

# Deezer Social Profile Enrichment — Overnight Runner
# Runs the enrichment in a continuous loop with a sleep between rounds.
#
# Usage:
#   ./overnight-deezer-social.sh                  # Default: 1000 per round
#   LIMIT=5000 ./overnight-deezer-social.sh       # Custom limit per round
#   nohup ./overnight-deezer-social.sh &           # Run in background

# Configuration
LIMIT=${LIMIT:-1000}
SLEEP_BETWEEN_ROUNDS=${SLEEP_BETWEEN_ROUNDS:-60}

echo "🎧 Deezer Social Profile Enrichment — Overnight Mode"
echo "======================================================"
echo "   Records per round: $LIMIT"
echo "   Sleep between rounds: ${SLEEP_BETWEEN_ROUNDS}s"
echo "======================================================"

ROUND=0

while true; do
    ROUND=$((ROUND + 1))
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "🔄 Round $ROUND — $(date +'%Y-%m-%d %H:%M:%S')"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    LIMIT=$LIMIT npx ts-node src/deezer-social-enrichment.ts
    
    echo ""
    echo "✨ Round $ROUND Complete! Sleeping ${SLEEP_BETWEEN_ROUNDS}s..."
    sleep $SLEEP_BETWEEN_ROUNDS
done
