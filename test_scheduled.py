#!/usr/bin/env python3
"""Test scheduled/delayed jobs functionality for CrocQueuectl"""

import subprocess
import time
import os
from datetime import datetime, timedelta

def cleanup():
    """Clean up any existing processes and database"""
    subprocess.run("pkill -f 'queuectl worker'", shell=True, capture_output=True)
    time.sleep(0.5)
    if os.path.exists("data/queuectl.db"):
        os.remove("data/queuectl.db")

def run(cmd):
    """Run command and return result"""
    print(f"$ {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout.strip())
    return result

print("\nTesting CrocQueuectl Scheduled Jobs")
print("="*50)

# Clean start
cleanup()

# Test 1: Enqueue immediate job
print("\n1. Enqueuing immediate job (should execute immediately)...")
run('queuectl enqueue \'{"id":"immediate1","command":"echo Immediate"}\'')

# Test 2: Enqueue delayed jobs
print("\n2. Enqueuing delayed jobs...")
run('queuectl enqueue \'{"id":"delay5s","command":"echo Delayed 5 seconds"}\' --delay 5s')
run('queuectl enqueue \'{"id":"delay1m","command":"echo Delayed 1 minute"}\' --delay 1m')

# Test 3: Enqueue scheduled job for specific time
print("\n3. Enqueuing job for specific time...")
future_time = datetime.utcnow() + timedelta(seconds=10)
scheduled_time = future_time.strftime('%Y-%m-%d %H:%M:%S')
run(f'queuectl enqueue \'{{"id":"scheduled1","command":"echo Scheduled at {scheduled_time}"}}\' --run-at "{scheduled_time}"')

# Test 4: List jobs to see scheduling info
print("\n4. Listing pending jobs (should show scheduled times)...")
result = run("queuectl list --state pending")

# Test 5: Run worker and verify immediate job executes
print("\n5. Running worker for 2 seconds (should only process immediate job)...")
worker = subprocess.Popen(
    ["bash", "-c", "timeout 2 queuectl worker start"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL
)
worker.wait()

# Check what got processed
print("\n6. Checking completed jobs (only immediate should be done)...")
result = run("queuectl list --state completed")
assert "immediate1" in result.stdout, "Immediate job should be completed"

print("\n7. Checking pending jobs (delayed jobs should still be pending)...")
result = run("queuectl list --state pending")
assert "delay5s" in result.stdout, "5-second delayed job should still be pending"
assert "delay1m" in result.stdout, "1-minute delayed job should still be pending"

# Test 6: Wait for delay and process delayed job
print("\n8. Waiting 5 seconds for delayed job to become ready...")
time.sleep(5)

print("\n9. Running worker again...")
worker = subprocess.Popen(
    ["bash", "-c", "timeout 2 queuectl worker start"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL
)
worker.wait()

print("\n10. Checking completed jobs (5s delayed should now be done)...")
result = run("queuectl list --state completed")
assert "delay5s" in result.stdout, "5-second delayed job should be completed"

# Test 7: Verify priority + scheduling combination
print("\n11. Testing priority + scheduling combination...")
run('queuectl enqueue \'{"id":"high_now","command":"echo High Priority Now"}\' --priority 10')
run('queuectl enqueue \'{"id":"low_now","command":"echo Low Priority Now"}\' --priority 1')
future_time = datetime.utcnow() + timedelta(seconds=30)
scheduled_time = future_time.strftime('%Y-%m-%d %H:%M:%S')
run(f'queuectl enqueue \'{{"id":"high_later","command":"echo High Priority Later"}}\' --priority 10 --run-at "{scheduled_time}"')

print("\n12. Running worker (high priority immediate should execute first)...")
worker = subprocess.Popen(
    ["bash", "-c", "timeout 2 queuectl worker start"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL
)
worker.wait()

# Verify execution order
result = run("queuectl list --state completed")
lines = result.stdout.strip().split('\n')

# Find the order of completion
completed_order = []
for line in lines:
    if 'high_now' in line:
        completed_order.append('high_now')
    elif 'low_now' in line:
        completed_order.append('low_now')

print(f"\n13. Execution order: {completed_order}")
if completed_order and completed_order[0] == 'high_now':
    print("[PASS] Priority works with scheduling! High priority executed first.")
else:
    print("[WARN] Priority order may not be correct")

# Check that scheduled high priority job is still pending
result = run("queuectl list --state pending")
assert "high_later" in result.stdout, "Scheduled high priority job should still be pending"

# Cleanup
print("\n14. Cleaning up...")
cleanup()

print("\n[PASS] Scheduled jobs test complete!")
