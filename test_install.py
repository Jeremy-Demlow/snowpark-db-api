#!/usr/bin/env python3
"""Simple test script to verify the installation works correctly."""

import sys
import traceback

def test_imports():
    """Test that all core modules can be imported."""
    try:
        print("Testing snowpark_db_api imports...")
        
        # Test core imports
        from snowpark_db_api import (
            DataTransfer, 
            transfer_data, 
            config_manager,
            DatabaseType,
            setup_logging
        )
        print("✓ Core imports successful")
        
        # Test configuration
        from snowpark_db_api.config import AppConfig, SqlServerConfig, SnowflakeConfig
        print("✓ Configuration imports successful")
        
        # Test connections
        from snowpark_db_api.connections import get_connection_factory
        print("✓ Connection imports successful")
        
        # Test CLI
        from snowpark_db_api.cli import app
        print("✓ CLI imports successful")
        
        print("\n🎉 All imports successful! The package is properly installed.")
        return True
        
    except Exception as e:
        print(f"❌ Import failed: {e}")
        traceback.print_exc()
        return False

def test_config():
    """Test basic configuration functionality."""
    try:
        print("\nTesting configuration...")
        
        # Test config manager
        config = config_manager.load_config()
        print("✓ Config manager works")
        
        # Test database types
        db_types = list(DatabaseType)
        print(f"✓ Supported database types: {[dt.value for dt in db_types]}")
        
        return True
        
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("=" * 60)
    print("SNOWPARK DB-API INSTALLATION TEST")
    print("=" * 60)
    
    success = True
    
    # Test imports
    if not test_imports():
        success = False
    
    # Test configuration
    if not test_config():
        success = False
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 ALL TESTS PASSED! The installation is working correctly.")
        print("\nNext steps:")
        print("1. Copy env-example to .env and fill in your credentials")
        print("2. Run: python -m snowpark_db_api.cli config-template")
        print("3. Run: python -m snowpark_db_api.cli transfer --help")
        print("4. Or use Docker: docker-compose up --build")
    else:
        print("❌ SOME TESTS FAILED. Please check the errors above.")
        sys.exit(1)
    
    print("=" * 60)

if __name__ == "__main__":
    main() 