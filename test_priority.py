#!/usr/bin/env python3
"""Test priority queue functionality for CrocQueuectl"""

import subprocess
import time
import os

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

print("\nTesting CrocQueuectl Priority Queues")
print("="*50)

# Clean start
cleanup()

# Test 1: Enqueue jobs with different priorities
print("\n1. Enqueuing jobs with different priorities...")
run('queuectl enqueue \'{"id":"low1","command":"echo Low Priority"}\' --priority 1')
run('queuectl enqueue \'{"id":"normal1","command":"echo Normal Priority"}\'')  # Default priority 5
run('queuectl enqueue \'{"id":"high1","command":"echo High Priority"}\' --priority 10')
run('queuectl enqueue \'{"id":"medium1","command":"echo Medium Priority"}\' --priority 5')
run('queuectl enqueue \'{"id":"urgent1","command":"echo Urgent"}\' --priority 9')

# Test 2: List jobs to see priority
print("\n2. Listing jobs (should show priority)...")
run("queuectl list --state pending")

# Test 3: Process jobs to verify priority order
print("\n3. Processing jobs (high priority should complete first)...")

# Create a test script that logs job execution order
with open("test_order.sh", "w") as f:
    f.write("#!/bin/bash\necho $1 >> job_order.txt\n")
os.chmod("test_order.sh", 0o755)

# Clear any existing order file
if os.path.exists("job_order.txt"):
    os.remove("job_order.txt")

# Enqueue more specific test jobs
print("\n4. Enqueueing test jobs to verify execution order...")
run('queuectl enqueue \'{"id":"p1","command":"./test_order.sh priority-1"}\' --priority 1')
run('queuectl enqueue \'{"id":"p10","command":"./test_order.sh priority-10"}\' --priority 10')
run('queuectl enqueue \'{"id":"p5","command":"./test_order.sh priority-5"}\' --priority 5')
run('queuectl enqueue \'{"id":"p8","command":"./test_order.sh priority-8"}\' --priority 8')
run('queuectl enqueue \'{"id":"p3","command":"./test_order.sh priority-3"}\' --priority 3')

# Run worker to process all jobs
print("\n5. Running worker to process jobs...")
worker = subprocess.Popen(
    ["bash", "-c", "timeout 3 queuectl worker start"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL
)
worker.wait()

# Check execution order
print("\n6. Checking execution order...")
if os.path.exists("job_order.txt"):
    with open("job_order.txt", "r") as f:
        order = f.read().strip().split("\n")
    print("Execution order was:")
    for i, job in enumerate(order, 1):
        print(f"  {i}. {job}")
    
    # Verify correct order (should be 10, 8, 5, 3, 1)
    expected = ["priority-10", "priority-8", "priority-5", "priority-3", "priority-1"]
    if order[:5] == expected:
        print("\n[PASS] Priority queue working correctly! Jobs executed in priority order.")
    else:
        print("\n[FAIL] Incorrect execution order!")
        print(f"Expected: {expected}")
        print(f"Got: {order[:5]}")
else:
    print("[WARN] No execution order file found")

# Test 7: Check completed jobs
print("\n7. Listing completed jobs...")
run("queuectl list --state completed")

# Cleanup
print("\n8. Cleaning up...")
if os.path.exists("test_order.sh"):
    os.remove("test_order.sh")
if os.path.exists("job_order.txt"):
    os.remove("job_order.txt")
cleanup()

print("\n[PASS] Priority queue test complete!")
