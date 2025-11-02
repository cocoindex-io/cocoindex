#!/usr/bin/env python3
"""Run main.py and capture output to test_output.txt"""

import subprocess
import sys

# Run main.py and capture output
result = subprocess.run(
    [sys.executable, "main.py"], capture_output=True, text=True, timeout=10
)

# Write both stdout and stderr to test_output.txt
with open("test_output.txt", "w") as f:
    if result.stdout:
        f.write(result.stdout)
    if result.stderr:
        f.write(result.stderr)

print(f"Output saved to test_output.txt")
print(f"Return code: {result.returncode}")
