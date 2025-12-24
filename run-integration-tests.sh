#!/bin/bash

# Integration Test Runner Script
# This script helps you run integration tests locally with Docker services

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
COMPOSE_FILE="docker-compose.test.yml"
MAX_WAIT=60

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}CCDI Portal Integration Test Runner${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        echo -e "${RED}Error: Docker is not running${NC}"
        echo "Please start Docker and try again"
        exit 1
    fi
    echo -e "${GREEN}✓${NC} Docker is running"
}

# Function to start services
start_services() {
    echo ""
    echo -e "${YELLOW}Starting test services...${NC}"
    docker-compose -f $COMPOSE_FILE up -d
    
    echo ""
    echo -e "${YELLOW}Waiting for services to be healthy...${NC}"
    
    # Wait for Neo4j
    echo -n "  Neo4j: "
    for i in $(seq 1 $MAX_WAIT); do
        if curl -s http://localhost:7474 > /dev/null 2>&1; then
            echo -e "${GREEN}✓ Ready${NC}"
            break
        fi
        if [ $i -eq $MAX_WAIT ]; then
            echo -e "${RED}✗ Timeout${NC}"
            exit 1
        fi
        sleep 1
    done
    
    # Wait for OpenSearch
    echo -n "  OpenSearch: "
    for i in $(seq 1 $MAX_WAIT); do
        if curl -s http://localhost:9200/_cluster/health > /dev/null 2>&1; then
            echo -e "${GREEN}✓ Ready${NC}"
            break
        fi
        if [ $i -eq $MAX_WAIT ]; then
            echo -e "${RED}✗ Timeout${NC}"
            exit 1
        fi
        sleep 1
    done
    
    # Wait for Redis
    echo -n "  Redis: "
    for i in $(seq 1 $MAX_WAIT); do
        if redis-cli -h localhost ping > /dev/null 2>&1; then
            echo -e "${GREEN}✓ Ready${NC}"
            break
        fi
        if [ $i -eq $MAX_WAIT ]; then
            echo -e "${RED}✗ Timeout${NC}"
            exit 1
        fi
        sleep 1
    done
}

# Function to stop services
stop_services() {
    echo ""
    echo -e "${YELLOW}Stopping test services...${NC}"
    docker-compose -f $COMPOSE_FILE down
    echo -e "${GREEN}✓${NC} Services stopped"
}

# Function to run tests
run_tests() {
    echo ""
    echo -e "${YELLOW}Running integration tests...${NC}"
    echo ""
    
    export NEO4J_URL=bolt://localhost:7687
    export NEO4J_USER=neo4j
    export NEO4J_PASSWORD=testpassword
    export ES_HOST=localhost
    export ES_PORT=9200
    export ES_SCHEME=http
    export REDIS_HOST=localhost
    export REDIS_PORT=6379
    
    if mvn verify -Dspring.profiles.active=integration; then
        echo ""
        echo -e "${GREEN}========================================${NC}"
        echo -e "${GREEN}✓ Integration tests passed!${NC}"
        echo -e "${GREEN}========================================${NC}"
    else
        echo ""
        echo -e "${RED}========================================${NC}"
        echo -e "${RED}✗ Integration tests failed${NC}"
        echo -e "${RED}========================================${NC}"
        exit 1
    fi
}

# Function to show service status
show_status() {
    echo ""
    echo -e "${YELLOW}Service Status:${NC}"
    echo ""
    echo "Neo4j:"
    curl -s http://localhost:7474 > /dev/null 2>&1 && echo -e "  ${GREEN}✓ Running${NC}" || echo -e "  ${RED}✗ Not running${NC}"
    echo ""
    echo "OpenSearch:"
    curl -s http://localhost:9200/_cluster/health?pretty 2>/dev/null || echo -e "  ${RED}✗ Not running${NC}"
    echo ""
    echo "Redis:"
    redis-cli -h localhost ping 2>/dev/null || echo -e "  ${RED}✗ Not running${NC}"
}

# Function to show logs
show_logs() {
    docker-compose -f $COMPOSE_FILE logs -f
}

# Function to clean up volumes
clean_volumes() {
    echo ""
    echo -e "${YELLOW}Cleaning up test data volumes...${NC}"
    docker-compose -f $COMPOSE_FILE down -v
    echo -e "${GREEN}✓${NC} Volumes cleaned"
}

# Main script
case "${1:-run}" in
    start)
        check_docker
        start_services
        show_status
        ;;
    stop)
        stop_services
        ;;
    run)
        check_docker
        start_services
        run_tests
        stop_services
        ;;
    test)
        run_tests
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    clean)
        check_docker
        stop_services
        clean_volumes
        ;;
    *)
        echo "Usage: $0 {start|stop|run|test|status|logs|clean}"
        echo ""
        echo "Commands:"
        echo "  start   - Start test services"
        echo "  stop    - Stop test services"
        echo "  run     - Start services, run tests, stop services (default)"
        echo "  test    - Run tests (assumes services are already running)"
        echo "  status  - Show service status"
        echo "  logs    - Show service logs"
        echo "  clean   - Stop services and remove volumes"
        exit 1
        ;;
esac

