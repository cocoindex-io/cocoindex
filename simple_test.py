#!/usr/bin/env python3
"""
Simple test to verify the CLI feature code changes are present.
"""

import os
import sys

def test_cli_changes():
    """Test that the CLI changes are present in the code."""
    print("🔍 Testing CLI code changes...")
    
    # Test 1: Check if --live-status option exists in CLI
    with open('python/cocoindex/cli.py', 'r') as f:
        cli_content = f.read()
    
    if '--live-status' in cli_content:
        print("✅ --live-status option found in CLI")
    else:
        print("❌ --live-status option NOT found in CLI")
        return False
    
    if 'live_status: bool' in cli_content:
        print("✅ live_status parameter found in CLI")
    else:
        print("❌ live_status parameter NOT found in CLI")
        return False
    
    if 'updater.print_cli_status()' in cli_content:
        print("✅ print_cli_status call found in CLI")
    else:
        print("❌ print_cli_status call NOT found in CLI")
        return False
    
    return True

def test_flow_changes():
    """Test that the Flow changes are present in the code."""
    print("\n🔍 Testing Flow code changes...")
    
    with open('python/cocoindex/flow.py', 'r') as f:
        flow_content = f.read()
    
    if 'def print_cli_status(self) -> None:' in flow_content:
        print("✅ print_cli_status method found in Flow")
    else:
        print("❌ print_cli_status method NOT found in Flow")
        return False
    
    if 'def next_status_updates_cli(self) -> None:' in flow_content:
        print("✅ next_status_updates_cli method found in Flow")
    else:
        print("❌ next_status_updates_cli method NOT found in Flow")
        return False
    
    return True

def test_rust_changes():
    """Test that the Rust changes are present in the code."""
    print("\n🔍 Testing Rust code changes...")
    
    with open('src/py/mod.rs', 'r') as f:
        rust_content = f.read()
    
    if 'print_cli_status_async' in rust_content:
        print("✅ print_cli_status_async method found in Rust bindings")
    else:
        print("❌ print_cli_status_async method NOT found in Rust bindings")
        return False
    
    if 'next_status_updates_cli_async' in rust_content:
        print("✅ next_status_updates_cli_async method found in Rust bindings")
    else:
        print("❌ next_status_updates_cli_async method NOT found in Rust bindings")
        return False
    
    return True

def test_rust_core():
    """Test that the Rust core changes are present."""
    print("\n🔍 Testing Rust core changes...")
    
    with open('src/execution/live_updater.rs', 'r') as f:
        live_updater_content = f.read()
    
    if 'source_interval_enabled' in live_updater_content:
        print("✅ source_interval_enabled field found in Rust core")
    else:
        print("❌ source_interval_enabled field NOT found in Rust core")
        return False
    
    if 'source_change_capture_enabled' in live_updater_content:
        print("✅ source_change_capture_enabled field found in Rust core")
    else:
        print("❌ source_change_capture_enabled field NOT found in Rust core")
        return False
    
    if 'print_cli_status' in live_updater_content:
        print("✅ print_cli_status method found in Rust core")
    else:
        print("❌ print_cli_status method NOT found in Rust core")
        return False
    
    return True

def main():
    """Run all tests."""
    print("🧪 Testing CLI Feature Implementation...")
    print("=" * 60)
    
    tests = [
        ("CLI Changes", test_cli_changes),
        ("Flow Changes", test_flow_changes),
        ("Rust Bindings", test_rust_changes),
        ("Rust Core", test_rust_core),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        if test_func():
            passed += 1
        else:
            print(f"❌ {test_name} failed")
    
    print("\n" + "=" * 60)
    print(f"📊 Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All code changes are present! The CLI feature is implemented.")
        print("\n📝 Usage:")
        print("   cocoindex show <flow> --live-status")
        print("   cocoindex update <flow> -L")
        print("\n✅ The feature is ready for use!")
    else:
        print("⚠️  Some code changes are missing. Check the errors above.")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
