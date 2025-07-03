#!/usr/bin/env python3
"""
Test runner for snowpark-db-api functional tests.
Focuses on real functionality testing instead of mocked unit tests.
"""

import sys
import subprocess
import os
from pathlib import Path

def run_command(cmd):
    """Run command and return success status."""
    print(f"Running: {cmd}")
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Command failed with exit code {e.returncode}")
        print("STDOUT:", e.stdout)
        print("STDERR:", e.stderr)
        return False

def run_smoke_tests():
    """Run smoke tests - quick validation."""
    print("=" * 60)
    print("🔥 SMOKE TESTS - Quick Validation")
    print("=" * 60)
    
    cmd = "python -m pytest tests/test_smoke.py -v -m smoke"
    return run_command(cmd)

def run_functional_tests():
    """Run functional tests - real end-to-end scenarios."""
    print("=" * 60)
    print("🚀 FUNCTIONAL TESTS - Real End-to-End Testing") 
    print("=" * 60)
    
    cmd = "python -m pytest tests/functional/ -v -m functional"
    return run_command(cmd)

def run_slow_tests():
    """Run slow tests - large data scenarios."""
    print("=" * 60)
    print("⏰ SLOW TESTS - Large Data Scenarios")
    print("=" * 60)
    
    cmd = "python -m pytest tests/functional/ -v -m slow"
    return run_command(cmd)

def run_all_tests():
    """Run all tests."""
    print("=" * 60)
    print("🎯 ALL TESTS")
    print("=" * 60)
    
    cmd = "python -m pytest tests/ -v"
    return run_command(cmd)

def run_coverage():
    """Run tests with coverage reporting."""
    print("=" * 60)
    print("📊 COVERAGE ANALYSIS")
    print("=" * 60)
    
    # Install coverage if not available
    try:
        import coverage
    except ImportError:
        print("Installing coverage...")
        run_command("pip install coverage")
    
    cmd = "coverage run -m pytest tests/test_smoke.py tests/functional/"
    success = run_command(cmd)
    
    if success:
        print("\n" + "=" * 40)
        print("📈 COVERAGE REPORT")
        print("=" * 40)
        run_command("coverage report -m --include='snowpark_db_api/*'")
        
        print("\n" + "=" * 40)
        print("📋 HTML COVERAGE REPORT")
        print("=" * 40)
        run_command("coverage html --include='snowpark_db_api/*'")
        print("📁 HTML report saved to: htmlcov/index.html")
    
    return success

def run_docker_tests():
    """Run tests in Docker container."""
    print("=" * 60)
    print("🐳 DOCKER TESTS")
    print("=" * 60)
    
    # Build testing container
    print("Building Docker testing image...")
    if not run_command("docker build --target testing -t snowpark-db-api-test ."):
        return False
    
    # Run smoke tests in container (always safe)
    print("Running smoke tests in Docker...")
    smoke_cmd = """docker run --rm \
        -v $(pwd)/test-results:/app/test-results \
        snowpark-db-api-test python run_tests.py smoke"""
    
    if not run_command(smoke_cmd):
        return False
    
    # Check if we have database credentials for functional tests
    env_vars = [
        'SOURCE_HOST', 'SOURCE_DATABASE', 'SOURCE_USERNAME', 'SOURCE_PASSWORD',
        'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_ROLE', 'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA'
    ]
    
    has_credentials = all(os.getenv(var) for var in env_vars[:4])  # Check at least source DB creds
    
    if has_credentials:
        print("Database credentials found - running functional tests...")
        functional_cmd = f"""docker run --rm \
            {' '.join([f'-e {var}' for var in env_vars])} \
            -v $(pwd)/test-results:/app/test-results \
            snowpark-db-api-test python run_tests.py functional"""
        
        return run_command(functional_cmd)
    else:
        print("No database credentials - skipping functional tests")
        print("To run functional tests in Docker, set these environment variables:")
        for var in env_vars:
            print(f"  {var}")
        return True

def run_cleanup():
    """Clean up old test databases."""
    print("=" * 60)
    print("🧹 CLEANING UP TEST DATABASES")
    print("=" * 60)
    
    try:
        from tests.test_database_manager import cleanup_old_test_databases
        cleanup_old_test_databases()
        print("✅ Cleanup completed successfully!")
        return True
    except Exception as e:
        print(f"❌ Cleanup failed: {e}")
        return False

def show_test_summary():
    """Show available test commands."""
    print("=" * 60)
    print("🧪 SNOWPARK-DB-API TEST RUNNER")
    print("=" * 60)
    print()
    print("Available test commands:")
    print()
    print("  smoke      - Quick validation tests (imports, basic functionality)")
    print("  functional - End-to-end functional tests (real database transfers)")
    print("  slow       - Large data transfer scenarios")
    print("  all        - Run all tests")
    print("  coverage   - Run tests with coverage analysis")
    print("  docker     - Run tests in Docker container")
    print("  cleanup    - Clean up old test databases")
    print("  help       - Show this help")
    print()
    print("Examples:")
    print("  python run_tests.py smoke")
    print("  python run_tests.py functional")
    print("  python run_tests.py coverage")
    print("  python run_tests.py cleanup")
    print()
    print("🔒 DATABASE ISOLATION:")
    print("  - Functional tests use isolated test databases")
    print("  - Test databases are created as DB_API_TEST_YYYYMMDD_HHMMSS")
    print("  - All test data is automatically cleaned up after testing")
    print("  - No pollution of production/development databases")
    print()
    print("Environment Requirements:")
    print("  - For functional tests: .env file with database credentials")
    print("  - For Docker tests: Docker daemon running")
    print()

def main():
    """Main test runner."""
    if len(sys.argv) < 2:
        show_test_summary()
        return
    
    command = sys.argv[1].lower()
    
    # Set PYTHONPATH to include project root
    project_root = Path(__file__).parent
    os.environ['PYTHONPATH'] = str(project_root)
    
    if command == "smoke":
        success = run_smoke_tests()
    elif command == "functional":
        success = run_functional_tests() 
    elif command == "slow":
        success = run_slow_tests()
    elif command == "all":
        success = run_all_tests()
    elif command == "coverage":
        success = run_coverage()
    elif command == "docker":
        success = run_docker_tests()
    elif command == "cleanup":
        success = run_cleanup()
    elif command in ["help", "-h", "--help"]:
        show_test_summary()
        return
    else:
        print(f"Unknown command: {command}")
        show_test_summary()
        sys.exit(1)
    
    if success:
        print()
        print("🎉 Tests completed successfully!")
        sys.exit(0)
    else:
        print()
        print("❌ Tests failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
