#!/bin/bash
# Script to check if the CLI feature is implemented

echo "Checking CLI feature implementation..."
echo "======================================"

echo ""
echo "1. Checking Rust core changes..."
if grep -q "source_interval_enabled" src/execution/live_updater.rs; then
    echo "   ✓ source_interval_enabled field found"
else
    echo "   ✗ source_interval_enabled field NOT found"
fi

if grep -q "source_change_capture_enabled" src/execution/live_updater.rs; then
    echo "   ✓ source_change_capture_enabled field found"
else
    echo "   ✗ source_change_capture_enabled field NOT found"
fi

if grep -q "print_cli_status" src/execution/live_updater.rs; then
    echo "   ✓ print_cli_status method found"
else
    echo "   ✗ print_cli_status method NOT found"
fi

echo ""
echo "2. Checking Python CLI changes..."
if grep -q "--live-status" python/cocoindex/cli.py; then
    echo "   ✓ --live-status option found"
else
    echo "   ✗ --live-status option NOT found"
fi

if grep -q "print_cli_status" python/cocoindex/flow.py; then
    echo "   ✓ print_cli_status method found in flow.py"
else
    echo "   ✗ print_cli_status method NOT found in flow.py"
fi

echo ""
echo "3. Checking Rust bindings..."
if grep -q "print_cli_status_async" src/py/mod.rs; then
    echo "   ✓ print_cli_status_async binding found"
else
    echo "   ✗ print_cli_status_async binding NOT found"
fi

echo ""
echo "4. Testing compilation..."
if cargo check --quiet 2>/dev/null; then
    echo "   ✓ Rust code compiles successfully"
else
    echo "   ✗ Rust code has compilation errors"
fi

echo ""
echo "======================================"
echo "Implementation check complete!"
