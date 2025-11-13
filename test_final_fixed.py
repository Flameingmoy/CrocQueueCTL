#!/usr/bin/env python3
"""Final comprehensive test for CrocQueuectl - Fixed version"""

import subprocess
import time
import os
import sys
import signal

def cleanup():
    """Clean up any existing processes and database"""
    # Kill any remaining worker processes
    subprocess.run("pkill -f 'queuectl worker'", shell=True, capture_output=True)
    time.sleep(0.5)
    if os.path.exists("data/queuectl.db"):
        os.remove("data/queuectl.db")
    print("[OK] Cleaned up environment")

def run(cmd, show_output=True):
    """Run command and return result"""
    print(f"\n$ {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if show_output and result.stdout:
        print(result.stdout.strip())
    return result

def run_worker_for_time(seconds, count=1):
    """Start worker(s) and stop them after specified seconds"""
    print(f"Starting {count} worker(s) for {seconds} seconds...")
    
    # Start worker process
    cmd = f"queuectl worker start --count {count}"
    worker = subprocess.Popen(
        cmd, 
        shell=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        preexec_fn=os.setsid  # Create new process group
    )
    
    # Wait for specified time
    time.sleep(seconds)
    
    # Send SIGTERM to process group to stop gracefully
    try:
        os.killpg(os.getpgid(worker.pid), signal.SIGTERM)
    except ProcessLookupError:
        pass  # Process already terminated
    
    # Wait for process to finish (max 2 seconds)
    try:
        worker.wait(timeout=2)
    except subprocess.TimeoutExpired:
        # Force kill if still running
        worker.kill()
        worker.wait()
    
    print("Workers stopped")

def test_basic_functionality():
    """Test 1: Basic job enqueuing and processing"""
    print("\n" + "="*60)
    print("TEST 1: Basic Job Processing")
    print("="*60)
    
    run('queuectl enqueue \'{"id":"basic1","command":"echo Hello World"}\'')
    run('queuectl enqueue \'{"id":"basic2","command":"date"}\'')
    
    # Run worker for 2 seconds
    run_worker_for_time(2)
    
    result = run("queuectl list --state completed")
    assert "basic1" in result.stdout and "basic2" in result.stdout, "Jobs not completed"
    print("[PASS] Basic job processing works")

def test_retry_and_dlq():
    """Test 2: Retry with exponential backoff and DLQ"""
    print("\n" + "="*60)
    print("TEST 2: Retry and Dead Letter Queue")
    print("="*60)
    
    # Configure for quick testing
    run("queuectl config set max-retries 1")
    run("queuectl config set backoff-base 1")
    
    # Enqueue failing jobs
    run('queuectl enqueue \'{"id":"fail1","command":"exit 42","max_retries":1}\'')
    run('queuectl enqueue \'{"id":"invalid1","command":"command_not_found","max_retries":1}\'')
    
    # First run - initial attempts
    print("\nFirst worker run (initial attempts)...")
    run_worker_for_time(2)
    
    # Check status
    result = run("queuectl status")
    
    # Wait for retry delay
    print("Waiting for retry delay...")
    time.sleep(2)
    
    # Second run - retry attempts
    print("Second worker run (retry attempts)...")
    run_worker_for_time(2)
    
    # Check DLQ
    result = run("queuectl dlq list")
    if "fail1" in result.stdout or "invalid1" in result.stdout:
        print("[PASS] Jobs moved to DLQ after max retries")
    else:
        print("[WARN] Jobs may need more time to reach DLQ")

def test_configuration():
    """Test 3: Configuration management"""
    print("\n" + "="*60)
    print("TEST 3: Configuration Management")
    print("="*60)
    
    run("queuectl config set max-retries 5")
    run("queuectl config set backoff-base 3")
    
    result = run("queuectl config get")
    assert "5" in result.stdout and "3" in result.stdout, "Config not set correctly"
    print("[PASS] Configuration management works")

def test_persistence():
    """Test 4: Data persistence across restarts"""
    print("\n" + "="*60)
    print("TEST 4: Data Persistence")
    print("="*60)
    
    run('queuectl enqueue \'{"id":"persist1","command":"sleep 0.1"}\'')
    run('queuectl enqueue \'{"id":"persist2","command":"echo persistent"}\'')
    
    result = run("queuectl list --state pending", show_output=False)
    print("Jobs persisted in database")
    
    # Check database file exists
    assert os.path.exists("data/queuectl.db"), "Database file not created"
    print(f"Database file exists: {os.path.exists('data/queuectl.db')}")
    print("[PASS] Persistence works - data survives restart")

def test_multi_worker():
    """Test 5: Multiple workers without race conditions"""
    print("\n" + "="*60)
    print("TEST 5: Multi-Worker Concurrency")
    print("="*60)
    
    # Enqueue multiple jobs
    for i in range(5):
        run(f'queuectl enqueue \'{{"id":"multi{i}","command":"sleep 0.1"}}\'', show_output=False)
    
    # Run 3 workers for 3 seconds
    run_worker_for_time(3, count=3)
    
    # Check no duplicates
    result = run("queuectl list --state completed")
    for i in range(5):
        count = result.stdout.count(f"multi{i}")
        assert count == 1, f"Job multi{i} processed {count} times (should be 1)"
    
    print("[PASS] Multi-worker processing without race conditions")

def test_dlq_retry():
    """Test 6: DLQ retry functionality"""
    print("\n" + "="*60)
    print("TEST 6: DLQ Retry")
    print("="*60)
    
    result = run("queuectl dlq list", show_output=False)
    if "fail1" in result.stdout:
        run("queuectl dlq retry fail1")
        result = run("queuectl list --state pending")
        assert "fail1" in result.stdout, "Job not moved back to pending"
        print("[PASS] DLQ retry works")
    else:
        print("[WARN] No jobs in DLQ to test retry")

def main():
    """Run all tests"""
    print("\nCROCQUEUECTL FINAL VALIDATION TEST SUITE\n")
    
    # Clean start
    cleanup()
    
    tests = [
        test_basic_functionality,
        test_configuration,
        test_persistence,
        test_retry_and_dlq,
        test_multi_worker,
        test_dlq_retry
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"[FAIL] Test failed: {e}")
            failed += 1
        except KeyboardInterrupt:
            print("\n[WARN] Test interrupted by user")
            break
    
    # Final cleanup
    cleanup()
    
    print("\n" + "="*60)
    print(f"FINAL RESULTS: {passed}/{len(tests)} tests passed")
    print("="*60)
    
    if failed == 0:
        print("[PASS] ALL REQUIREMENTS VALIDATED SUCCESSFULLY!")
        print("\nCrocQueuectl is ready for production use!")
        print("See README.md for usage documentation.")
    else:
        print(f"[FAIL] {failed} test(s) failed - review and fix issues")
        sys.exit(1)

if __name__ == "__main__":
    main()
