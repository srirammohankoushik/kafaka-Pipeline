BOOTSTRAP="localhost:9092"
KAFKA_BIN="/opt/kafka/bin"
SAMPLE=1000       # number of records to check 
PASS=0
FAIL=0

echo ""
echo "============================================"
echo " Verification: Kafka Pipeline Output"
echo "============================================"
echo ""

# ── 1. Record counts ──────────────────────────────────────────────

echo "--- Step 1: Record Counts ---"
for TOPIC in source id name continent; do
  COUNT=$($KAFKA_BIN/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list $BOOTSTRAP --topic $TOPIC --time -1 2>/dev/null | \
    awk -F: '{sum+=$3}END{print sum}')
  echo "  $TOPIC: $COUNT records"
done
echo ""

# ── 2. Verify ID topic (numerical sort) ──────────────────────────

echo "--- Step 2: Verify ID topic (first $SAMPLE records, numerical sort) ---"
$KAFKA_BIN/kafka-console-consumer.sh \
  --bootstrap-server $BOOTSTRAP \
  --topic id \
  --from-beginning \
  --max-messages $SAMPLE \
  --timeout-ms 10000 2>/dev/null | \
awk -F, '
BEGIN { prev = -1; errors = 0 }
{
  cur = $1 + 0
  if (prev > cur) {
    errors++
    if (errors <= 3) printf "  ERROR: id %d came after %d (line %d)\n", cur, prev, NR
  }
  prev = cur
}
END {
  if (errors == 0) print "  ✓ ID sort PASSED (" NR " records checked)"
  else print "  ✗ ID sort FAILED (" errors " out-of-order in " NR " records)"
}' > /tmp/id_result.txt 2>&1
cat /tmp/id_result.txt
if grep -q "PASSED" /tmp/id_result.txt; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi
echo ""

# ── 3. Verify NAME topic (alphabetical sort) ─────────────────────

echo "--- Step 3: Verify NAME topic (first $SAMPLE records, alphabetical sort) ---"
$KAFKA_BIN/kafka-console-consumer.sh \
  --bootstrap-server $BOOTSTRAP \
  --topic name \
  --from-beginning \
  --max-messages $SAMPLE \
  --timeout-ms 10000 2>/dev/null | \
awk -F, '
BEGIN { prev = ""; errors = 0 }
{
  cur = $2
  if (prev != "" && prev > cur) {
    errors++
    if (errors <= 3) printf "  ERROR: name \"%s\" came after \"%s\" (line %d)\n", cur, prev, NR
  }
  prev = cur
}
END {
  if (errors == 0) print "  ✓ NAME sort PASSED (" NR " records checked)"
  else print "  ✗ NAME sort FAILED (" errors " out-of-order in " NR " records)"
}' > /tmp/name_result.txt 2>&1
cat /tmp/name_result.txt
if grep -q "PASSED" /tmp/name_result.txt; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi
echo ""

# ── 4. Verify CONTINENT topic (alphabetical sort) ────────────────

echo "--- Step 4: Verify CONTINENT topic (first $SAMPLE records, alphabetical sort) ---"
$KAFKA_BIN/kafka-console-consumer.sh \
  --bootstrap-server $BOOTSTRAP \
  --topic continent \
  --from-beginning \
  --max-messages $SAMPLE \
  --timeout-ms 10000 2>/dev/null | \
awk -F, '
BEGIN { prev = ""; errors = 0 }
{
  cur = $4
  if (prev != "" && prev > cur) {
    errors++
    if (errors <= 3) printf "  ERROR: continent \"%s\" came after \"%s\" (line %d)\n", cur, prev, NR
  }
  prev = cur
}
END {
  if (errors == 0) print "  ✓ CONTINENT sort PASSED (" NR " records checked)"
  else print "  ✗ CONTINENT sort FAILED (" errors " out-of-order in " NR " records)"
}' > /tmp/cont_result.txt 2>&1
cat /tmp/cont_result.txt
if grep -q "PASSED" /tmp/cont_result.txt; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi
echo ""

# ── 5. Sample output (first 5 records from each) ─────────────────

echo "--- Sample Output (first 5 records) ---"
echo ""
echo "ID topic:"
$KAFKA_BIN/kafka-console-consumer.sh \
  --bootstrap-server $BOOTSTRAP --topic id \
  --from-beginning --max-messages 5 --timeout-ms 5000 2>/dev/null
echo ""
echo "NAME topic:"
$KAFKA_BIN/kafka-console-consumer.sh \
  --bootstrap-server $BOOTSTRAP --topic name \
  --from-beginning --max-messages 5 --timeout-ms 5000 2>/dev/null
echo ""
echo "CONTINENT topic:"
$KAFKA_BIN/kafka-console-consumer.sh \
  --bootstrap-server $BOOTSTRAP --topic continent \
  --from-beginning --max-messages 5 --timeout-ms 5000 2>/dev/null
echo ""

# ── Summary ───────────────────────────────────────────────────────

echo "============================================"
echo " Verification Summary"
echo "============================================"
echo "  Passed: $PASS / 3"
echo "  Failed: $FAIL / 3"
if [ $FAIL -eq 0 ]; then
  echo "  Result: ALL CHECKS PASSED ✓"
else
  echo "  Result: SOME CHECKS FAILED ✗"
fi
echo "============================================"

# Clean up temp files
rm -f /tmp/id_result.txt /tmp/name_result.txt /tmp/cont_result.txt
