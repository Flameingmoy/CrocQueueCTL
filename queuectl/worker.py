"""Worker process management module"""

import os
import signal
import time
import uuid
import logging
import multiprocessing
from typing import Optional
from datetime import datetime

from .database import Database
from .job import JobProcessor
from .config import Config

logger = logging.getLogger(__name__)


class Worker:
    """Individual worker process that executes jobs"""
    
    def __init__(self, worker_id: str = None, db_path: str = None):
        self.worker_id = worker_id or str(uuid.uuid4())
        self.pid = os.getpid()
        self.db = Database(db_path) if db_path else Database()
        self.config = Config(self.db)
        self.processor = JobProcessor(self.db)
        self.running = False
        self.shutdown_requested = False
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)
    
    def _handle_signal(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Worker {self.worker_id} received signal {signum}, shutting down...")
        self.shutdown_requested = True
    
    def start(self):
        """Start the worker process"""
        logger.info(f"Worker {self.worker_id} (PID: {self.pid}) starting...")
        
        # Register worker in database
        self.db.register_worker(self.worker_id, self.pid)
        
        self.running = True
        last_heartbeat = time.time()
        
        try:
            while self.running and not self.shutdown_requested:
                # Send heartbeat periodically
                if time.time() - last_heartbeat > self.config.get('worker_heartbeat_interval'):
                    self.db.update_worker_heartbeat(self.worker_id)
                    last_heartbeat = time.time()
                
                # Try to acquire and process a job
                job = self.db.acquire_job(self.worker_id)
                
                if job:
                    logger.info(f"Worker {self.worker_id} acquired job {job['id']}")
                    try:
                        self.processor.process_job(job, self.worker_id)
                    except Exception as e:
                        logger.error(f"Worker {self.worker_id} error processing job {job['id']}: {e}")
                        # Mark job as failed
                        self.db.update_job_state(job['id'], 'failed', error_message=str(e))
                else:
                    # No jobs available, wait before polling again
                    time.sleep(self.config.get('worker_poll_interval'))
        
        except Exception as e:
            logger.error(f"Worker {self.worker_id} unexpected error: {e}")
        
        finally:
            # Clean up and mark worker as stopped
            self.stop()
    
    def stop(self):
        """Stop the worker process"""
        logger.info(f"Worker {self.worker_id} stopping...")
        self.running = False
        self.db.mark_worker_stopped(self.worker_id)


class WorkerManager:
    """Manages multiple worker processes"""
    
    def __init__(self, db_path: str = None):
        self.db = Database(db_path) if db_path else Database()
        self.config = Config(self.db)
        self.processes = []
        self.running = False
    
    def start_workers(self, count: int = 1, daemon: bool = False):
        """Start multiple worker processes"""
        logger.info(f"Starting {count} worker(s)...")
        
        # Clean up any stale workers first
        stale_count = self.db.cleanup_stale_workers()
        if stale_count > 0:
            logger.info(f"Cleaned up {stale_count} stale workers")
        
        self.running = True
        
        for i in range(count):
            worker_id = f"worker-{uuid.uuid4().hex[:8]}"
            process = multiprocessing.Process(
                target=self._run_worker,
                args=(worker_id, self.db.db_path)
            )
            # Set daemon flag - process will terminate when parent exits
            # But we track it in DB so we can manage it separately
            if daemon:
                process.daemon = False  # Keep as non-daemon but detach
            process.start()
            self.processes.append((worker_id, process))
            logger.info(f"Started worker {worker_id} (PID: {process.pid})")
            
            # Store PID in database for tracking
            self.db.register_worker(worker_id, process.pid)
        
        return len(self.processes)
    
    def _run_worker(self, worker_id: str, db_path: str):
        """Run a single worker in a separate process"""
        # Detach from parent process group (for daemon mode)
        try:
            os.setsid()
        except:
            pass  # May fail on some systems, that's OK
        
        # Configure logging for worker process
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        worker = Worker(worker_id, db_path)
        worker.start()
    
    def stop_workers(self):
        """Stop all worker processes gracefully"""
        logger.info("Stopping all workers...")
        
        # Send SIGTERM to all processes
        for worker_id, process in self.processes:
            if process.is_alive():
                logger.info(f"Sending SIGTERM to worker {worker_id} (PID: {process.pid})")
                process.terminate()
        
        # Wait for processes to finish gracefully (max 10 seconds)
        start_time = time.time()
        timeout = 10
        
        while time.time() - start_time < timeout:
            alive_count = sum(1 for _, p in self.processes if p.is_alive())
            if alive_count == 0:
                break
            time.sleep(0.5)
        
        # Force kill any remaining processes
        for worker_id, process in self.processes:
            if process.is_alive():
                logger.warning(f"Force killing worker {worker_id} (PID: {process.pid})")
                process.kill()
                process.join()
        
        self.processes.clear()
        self.running = False
        logger.info("All workers stopped")
    
    def get_status(self) -> dict:
        """Get status of all workers"""
        active_workers = self.db.get_active_workers()
        return {
            'active_count': len(active_workers),
            'workers': active_workers
        }
