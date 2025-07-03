#!/bin/bash
# Docker Testing Script for Snowpark DB-API
# Makes it easy to run tests in Docker containers

set -e

echo "🐳 Snowpark DB-API Docker Testing"
echo "=================================="

# Create test results directory
mkdir -p test-results htmlcov

# Function to show usage
show_usage() {
    echo ""
    echo "Usage: ./docker-test.sh [command]"
    echo ""
    echo "Commands:"
    echo "  smoke       - Run smoke tests (no DB credentials needed)"
    echo "  functional  - Run functional tests (requires .env file)"
    echo "  coverage    - Run tests with coverage analysis"
    echo "  dev         - Development mode with local code mounting"
    echo "  build       - Build testing image only"
    echo "  clean       - Clean up test containers and images"
    echo "  all         - Run all tests (smoke + functional + coverage)"
    echo ""
    echo "Examples:"
    echo "  ./docker-test.sh smoke"
    echo "  ./docker-test.sh functional"
    echo "  ./docker-test.sh coverage"
    echo ""
}

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose not found. Please install docker-compose."
    exit 1
fi

# Get command
COMMAND=${1:-help}

case $COMMAND in
    "smoke")
        echo "🔥 Running smoke tests..."
        docker-compose -f docker-compose.test.yml --profile smoke up --build --abort-on-container-exit
        ;;
    
    "functional")
        echo "🚀 Running functional tests..."
        if [ ! -f .env ]; then
            echo "⚠️  No .env file found. Functional tests require database credentials."
            echo "   Create a .env file with your database credentials."
            exit 1
        fi
        
        # Load .env file
        export $(cat .env | grep -v '^#' | xargs)
        
        docker-compose -f docker-compose.test.yml --profile functional up --build --abort-on-container-exit
        ;;
    
    "coverage")
        echo "📊 Running tests with coverage analysis..."
        if [ ! -f .env ]; then
            echo "⚠️  No .env file found. Coverage tests work better with database credentials."
            echo "   Will run smoke tests only for coverage."
        else
            # Load .env file
            export $(cat .env | grep -v '^#' | xargs)
        fi
        
        docker-compose -f docker-compose.test.yml --profile coverage up --build --abort-on-container-exit
        
        echo ""
        echo "📈 Coverage report available at: htmlcov/index.html"
        ;;
    
    "dev")
        echo "🛠️  Starting development testing environment..."
        docker-compose -f docker-compose.test.yml --profile dev up --build
        ;;
    
    "build")
        echo "🔨 Building testing image..."
        docker build --target testing -t snowpark-db-api-test .
        ;;
    
    "clean")
        echo "🧹 Cleaning up test containers and images..."
        docker-compose -f docker-compose.test.yml down --rmi all --volumes --remove-orphans
        docker system prune -f
        ;;
    
    "all")
        echo "🎯 Running all tests..."
        
        # Always run smoke tests
        echo ""
        echo "Step 1/3: Smoke tests"
        docker-compose -f docker-compose.test.yml --profile smoke up --build --abort-on-container-exit
        
        # Run functional tests if .env exists
        if [ -f .env ]; then
            echo ""
            echo "Step 2/3: Functional tests"
            export $(cat .env | grep -v '^#' | xargs)
            docker-compose -f docker-compose.test.yml --profile functional up --build --abort-on-container-exit
            
            echo ""
            echo "Step 3/3: Coverage analysis"
            docker-compose -f docker-compose.test.yml --profile coverage up --build --abort-on-container-exit
            
            echo ""
            echo "📈 Coverage report available at: htmlcov/index.html"
        else
            echo ""
            echo "⚠️  Skipping functional tests - no .env file found"
            echo "Step 2/2: Coverage analysis (smoke tests only)"
            docker-compose -f docker-compose.test.yml --profile coverage up --build --abort-on-container-exit
        fi
        ;;
    
    "help"|"-h"|"--help"|*)
        show_usage
        exit 0
        ;;
esac

echo ""
echo "✅ Docker testing completed!"
echo "📁 Test results saved to: test-results/"
echo "📊 Coverage reports saved to: htmlcov/" 