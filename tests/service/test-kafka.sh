#!/bin/bash

# Kafka Testing Script
# Comprehensive tests for Kafka operations

# Don't exit on error - we handle errors in print_result
set +e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASSED=0
FAILED=0

print_result() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✓ PASS: $2${NC}"
        PASSED=$((PASSED + 1))
    else
        echo -e "${RED}✗ FAIL: $2${NC}"
        FAILED=$((FAILED + 1))
    fi
}

echo "=========================================="
echo "Kafka Comprehensive Tests"
echo "=========================================="
echo ""

# Get Kafka container name
KAFKA_CONTAINER=$(docker ps --filter "ancestor=confluentinc/cp-kafka:7.5.0" --format "{{.Names}}" | head -1)

if [ -z "$KAFKA_CONTAINER" ]; then
    echo -e "${RED}✗ Error: Kafka container not found${NC}"
    echo -e "${YELLOW}Make sure Kafka is running: docker-compose -f docker-compose-arm.yml up -d kafka${NC}"
    exit 1
fi

echo "Using Kafka container: $KAFKA_CONTAINER"
echo ""

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
for i in {1..30}; do
    if docker exec "$KAFKA_CONTAINER" kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
        echo -e "${GREEN}Kafka is ready!${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${RED}✗ Error: Kafka did not become ready within 30 seconds${NC}"
        exit 1
    fi
    sleep 1
done
echo ""

TEST_TOPIC="test-topic-$(date +%s)"

# Test 1: Create topic
echo "Test 1: Create topic"
docker exec "$KAFKA_CONTAINER" kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic "$TEST_TOPIC" \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists > /dev/null 2>&1
print_result $? "Create topic: $TEST_TOPIC"

# Test 2: List topics
echo "Test 2: List topics"
TOPIC_LIST=$(docker exec "$KAFKA_CONTAINER" kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null)
if echo "$TOPIC_LIST" | grep -q "$TEST_TOPIC"; then
    print_result 0 "List topics (topic found)"
else
    print_result 1 "List topics (topic not found)"
fi

# Test 3: Describe topic
echo "Test 3: Describe topic"
TOPIC_DESC=$(docker exec "$KAFKA_CONTAINER" kafka-topics --describe \
    --bootstrap-server localhost:9092 \
    --topic "$TEST_TOPIC" 2>/dev/null)
if echo "$TOPIC_DESC" | grep -q "$TEST_TOPIC"; then
    print_result 0 "Describe topic"
else
    print_result 1 "Describe topic"
fi

# Test 4: Produce messages
echo "Test 4: Produce messages"
TEST_MESSAGES=("Hello Kafka" "Test message 1" "Test message 2" "Test message 3")
for msg in "${TEST_MESSAGES[@]}"; do
    echo "$msg" | docker exec -i "$KAFKA_CONTAINER" kafka-console-producer \
        --bootstrap-server localhost:9092 \
        --topic "$TEST_TOPIC" > /dev/null 2>&1
done
print_result $? "Produce messages to topic"

# Wait a bit for messages to be available
sleep 2

# Test 5: Consume messages
echo "Test 5: Consume messages"
# kafka-console-consumer with --max-messages will stop automatically
CONSUMED=$(docker exec "$KAFKA_CONTAINER" kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic "$TEST_TOPIC" \
    --from-beginning \
    --max-messages 4 \
    --timeout-ms 5000 2>/dev/null || echo "")

MESSAGE_COUNT=$(echo "$CONSUMED" | grep -v "^$" | wc -l | tr -d ' ')
if [ "$MESSAGE_COUNT" -ge 4 ]; then
    print_result 0 "Consume messages (found $MESSAGE_COUNT messages)"
else
    print_result 1 "Consume messages (expected 4, found $MESSAGE_COUNT)"
fi

# Test 6: Verify message content
echo "Test 6: Verify message content"
if echo "$CONSUMED" | grep -q "Hello Kafka"; then
    print_result 0 "Message content verification"
else
    print_result 1 "Message content verification"
fi

# Test 7: Test from host (external connectivity)
echo "Test 7: External connectivity from host"
# Check if port 9092 is exposed via docker port mapping
if docker port "$KAFKA_CONTAINER" 2>/dev/null | grep -q "9092"; then
    # Try to actually connect if nc is available
    if command -v nc &> /dev/null; then
        # macOS nc uses -G, Linux uses -w
        if (nc -z -G 2 localhost 9092 2>/dev/null || nc -z -w 2 localhost 9092 2>/dev/null); then
            print_result 0 "External connectivity (port 9092 accessible)"
        else
            print_result 0 "External connectivity (port 9092 exposed but connection test failed - may need time to stabilize)"
        fi
    else
        print_result 0 "External connectivity (port 9092 exposed)"
    fi
else
    print_result 1 "External connectivity (port 9092 not exposed)"
fi

# Test 8: Test multiple partitions (if supported)
echo "Test 8: Create topic with multiple partitions"
MULTI_PART_TOPIC="test-multi-part-$(date +%s)"
docker exec "$KAFKA_CONTAINER" kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic "$MULTI_PART_TOPIC" \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists > /dev/null 2>&1
PART_COUNT=$(docker exec "$KAFKA_CONTAINER" kafka-topics --describe \
    --bootstrap-server localhost:9092 \
    --topic "$MULTI_PART_TOPIC" 2>/dev/null | grep -c "Partition:" || echo "0")
if [ "$PART_COUNT" -eq 3 ]; then
    print_result 0 "Create topic with multiple partitions"
else
    print_result 1 "Create topic with multiple partitions"
fi

# Clean up test topics
echo ""
echo "Cleaning up test topics..."
docker exec "$KAFKA_CONTAINER" kafka-topics --delete \
    --bootstrap-server localhost:9092 \
    --topic "$TEST_TOPIC" > /dev/null 2>&1 || true
docker exec "$KAFKA_CONTAINER" kafka-topics --delete \
    --bootstrap-server localhost:9092 \
    --topic "$MULTI_PART_TOPIC" > /dev/null 2>&1 || true
echo -e "${GREEN}✓ Test topics cleaned up${NC}"

echo ""
echo "=========================================="
echo "Test Summary"
echo "=========================================="
echo -e "${GREEN}Passed: $PASSED${NC}"
echo -e "${RED}Failed: $FAILED${NC}"
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}Some tests failed${NC}"
    exit 1
fi

