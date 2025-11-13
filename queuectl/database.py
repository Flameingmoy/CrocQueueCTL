"""Database layer for queuectl using SQLite"""

import sqlite3
import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "queuectl.db")


class Database:
    """SQLite database wrapper with concurrency-safe operations"""
    
    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_db()
    
    @contextmanager
    def get_connection(self):
        """Get a database connection with proper settings for concurrency"""
        conn = sqlite3.connect(self.db_path, isolation_level=None, timeout=10.0)
        conn.row_factory = sqlite3.Row
        try:
            # Configure for concurrent access
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA busy_timeout = 5000")
            yield conn
        finally:
            conn.close()
    
    def _init_db(self):
        """Initialize database schema"""
        with self.get_connection() as conn:
            # Jobs table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    command TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3,
                    backoff_base INTEGER DEFAULT 2,
                    priority INTEGER DEFAULT 5,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    next_retry_at TIMESTAMP,
                    run_at TIMESTAMP,
                    error_message TEXT,
                    worker_id TEXT,
                    output TEXT
                )
            """)
            
            # Index for efficient job queries
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_state_priority_retry 
                ON jobs (state, priority DESC, next_retry_at)
            """)
            
            # Configuration table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS config (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            
            # Workers table for tracking active workers
            conn.execute("""
                CREATE TABLE IF NOT EXISTS workers (
                    id TEXT PRIMARY KEY,
                    pid INTEGER NOT NULL,
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'running',
                    current_job_id TEXT
                )
            """)
            
            # Migrate existing tables if needed
            self._migrate_schema(conn)
            
            # Set default config values if not present
            self._init_config(conn)
    
    def _migrate_schema(self, conn):
        """Migrate existing database schema to add new columns"""
        # Check if priority column exists
        cursor = conn.execute("PRAGMA table_info(jobs)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'priority' not in columns:
            # Add priority column to existing database
            conn.execute("ALTER TABLE jobs ADD COLUMN priority INTEGER DEFAULT 5")
            logger.info("Added priority column to jobs table")
        
        if 'run_at' not in columns:
            # Add run_at column for scheduled jobs
            conn.execute("ALTER TABLE jobs ADD COLUMN run_at TIMESTAMP")
            logger.info("Added run_at column to jobs table")
    
    def _init_config(self, conn):
        """Initialize default configuration values"""
        defaults = {
            'max_retries': '3',
            'backoff_base': '2'
        }
        for key, value in defaults.items():
            conn.execute(
                "INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)",
                (key, value)
            )
    
    def enqueue_job(self, job_data: Dict[str, Any]) -> bool:
        """Add a new job to the queue with optional priority and scheduling"""
        try:
            with self.get_connection() as conn:
                conn.execute("BEGIN TRANSACTION")
                conn.execute("""
                    INSERT INTO jobs (id, command, state, max_retries, backoff_base, priority, run_at)
                    VALUES (?, ?, 'pending', ?, ?, ?, ?)
                """, (
                    job_data['id'],
                    job_data['command'],
                    job_data.get('max_retries', 3),
                    job_data.get('backoff_base', 2),
                    job_data.get('priority', 5),  # Default priority is 5 (medium)
                    job_data.get('run_at')  # Can be None for immediate jobs
                ))
                conn.execute("COMMIT")
                if job_data.get('run_at'):
                    logger.info(f"Job {job_data['id']} scheduled for {job_data['run_at']} with priority {job_data.get('priority', 5)}")
                else:
                    logger.info(f"Job {job_data['id']} enqueued with priority {job_data.get('priority', 5)}")
                return True
        except sqlite3.IntegrityError:
            logger.error(f"Job {job_data['id']} already exists")
            return False
        except Exception as e:
            logger.error(f"Failed to enqueue job: {e}")
            return False
    
    def acquire_job(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """
        Atomically acquire a pending job for processing.
        This is the critical race-condition-free pattern.
        """
        with self.get_connection() as conn:
            try:
                # Begin immediate transaction to get write lock upfront
                conn.execute("BEGIN IMMEDIATE TRANSACTION")
                
                # Find next available job (highest priority first)
                cursor = conn.execute("""
                    SELECT * FROM jobs 
                    WHERE state = 'pending' 
                        AND (next_retry_at IS NULL OR next_retry_at <= datetime('now'))
                        AND (run_at IS NULL OR run_at <= datetime('now'))
                    ORDER BY priority DESC, created_at ASC 
                    LIMIT 1
                """)
                job = cursor.fetchone()
                
                if job:
                    # Atomically update job state
                    conn.execute("""
                        UPDATE jobs 
                        SET state = 'processing', 
                            worker_id = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ? AND state = 'pending'
                    """, (worker_id, job['id']))
                    
                    # Check if update was successful
                    if conn.total_changes > 0:
                        conn.execute("COMMIT")
                        return dict(job)
                    else:
                        # Job was already taken by another worker
                        conn.execute("ROLLBACK")
                        return None
                else:
                    conn.execute("ROLLBACK")
                    return None
                    
            except Exception as e:
                conn.execute("ROLLBACK")
                logger.error(f"Failed to acquire job: {e}")
                return None
    
    def update_job_state(self, job_id: str, state: str, error_message: str = None, 
                        output: str = None, next_retry_at: str = None) -> bool:
        """Update job state and related fields"""
        try:
            with self.get_connection() as conn:
                conn.execute("BEGIN TRANSACTION")
                
                # If job is being retried (pending with error), increment attempts
                if state == 'pending' and error_message is not None:
                    conn.execute("""
                        UPDATE jobs 
                        SET state = ?, 
                            error_message = ?,
                            output = ?,
                            attempts = attempts + 1,
                            next_retry_at = ?,
                            updated_at = CURRENT_TIMESTAMP,
                            worker_id = NULL
                        WHERE id = ?
                    """, (state, error_message, output, next_retry_at, job_id))
                elif state == 'failed':
                    # Legacy support for failed state with retry
                    conn.execute("""
                        UPDATE jobs 
                        SET state = ?, 
                            error_message = ?,
                            output = ?,
                            attempts = attempts + 1,
                            next_retry_at = ?,
                            updated_at = CURRENT_TIMESTAMP,
                            worker_id = NULL
                        WHERE id = ?
                    """, (state, error_message, output, next_retry_at, job_id))
                else:
                    # Normal state update (completed, dead, or pending without error)
                    conn.execute("""
                        UPDATE jobs 
                        SET state = ?, 
                            error_message = ?,
                            output = ?,
                            next_retry_at = ?,
                            updated_at = CURRENT_TIMESTAMP,
                            worker_id = NULL
                        WHERE id = ?
                    """, (state, error_message, output, next_retry_at, job_id))
                
                conn.execute("COMMIT")
                return True
        except Exception as e:
            logger.error(f"Failed to update job state: {e}")
            return False
    
    def move_to_dlq(self, job_id: str, error_message: str = None) -> bool:
        """Move a job to the dead letter queue"""
        return self.update_job_state(job_id, 'dead', error_message)
    
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific job by ID"""
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def list_jobs(self, state: str = None) -> List[Dict[str, Any]]:
        """List jobs, optionally filtered by state"""
        with self.get_connection() as conn:
            if state:
                cursor = conn.execute(
                    "SELECT * FROM jobs WHERE state = ? ORDER BY created_at DESC",
                    (state,)
                )
            else:
                cursor = conn.execute("SELECT * FROM jobs ORDER BY created_at DESC")
            return [dict(row) for row in cursor.fetchall()]
    
    def get_status_summary(self) -> Dict[str, int]:
        """Get summary count of jobs by state"""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT state, COUNT(*) as count 
                FROM jobs 
                GROUP BY state
            """)
            return {row['state']: row['count'] for row in cursor.fetchall()}
    
    def get_dlq_jobs(self) -> List[Dict[str, Any]]:
        """Get all jobs in the dead letter queue"""
        return self.list_jobs(state='dead')
    
    def retry_dlq_job(self, job_id: str) -> bool:
        """Move a job from DLQ back to pending"""
        try:
            with self.get_connection() as conn:
                conn.execute("BEGIN TRANSACTION")
                conn.execute("""
                    UPDATE jobs 
                    SET state = 'pending', 
                        attempts = 0,
                        error_message = NULL,
                        next_retry_at = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ? AND state = 'dead'
                """, (job_id,))
                
                if conn.total_changes > 0:
                    conn.execute("COMMIT")
                    return True
                else:
                    conn.execute("ROLLBACK")
                    return False
        except Exception as e:
            logger.error(f"Failed to retry DLQ job: {e}")
            return False
    
    def get_config(self, key: str) -> Optional[str]:
        """Get configuration value"""
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT value FROM config WHERE key = ?", (key,))
            row = cursor.fetchone()
            return row['value'] if row else None
    
    def set_config(self, key: str, value: str) -> bool:
        """Set configuration value"""
        try:
            with self.get_connection() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
                    (key, value)
                )
                return True
        except Exception as e:
            logger.error(f"Failed to set config: {e}")
            return False
    
    def register_worker(self, worker_id: str, pid: int) -> bool:
        """Register a new worker"""
        try:
            with self.get_connection() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO workers (id, pid, status)
                    VALUES (?, ?, 'running')
                """, (worker_id, pid))
                return True
        except Exception as e:
            logger.error(f"Failed to register worker: {e}")
            return False
    
    def update_worker_heartbeat(self, worker_id: str, current_job_id: str = None) -> bool:
        """Update worker heartbeat and current job"""
        try:
            with self.get_connection() as conn:
                conn.execute("""
                    UPDATE workers 
                    SET last_heartbeat = CURRENT_TIMESTAMP,
                        current_job_id = ?
                    WHERE id = ?
                """, (current_job_id, worker_id))
                return True
        except Exception as e:
            logger.error(f"Failed to update worker heartbeat: {e}")
            return False
    
    def mark_worker_stopped(self, worker_id: str) -> bool:
        """Mark a worker as stopped"""
        try:
            with self.get_connection() as conn:
                conn.execute("""
                    UPDATE workers 
                    SET status = 'stopped',
                        current_job_id = NULL
                    WHERE id = ?
                """, (worker_id,))
                return True
        except Exception as e:
            logger.error(f"Failed to mark worker as stopped: {e}")
            return False
    
    def get_active_workers(self) -> List[Dict[str, Any]]:
        """Get list of active workers"""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM workers 
                WHERE status = 'running' 
                ORDER BY started_at DESC
            """)
            return [dict(row) for row in cursor.fetchall()]
    
    def cleanup_stale_workers(self) -> int:
        """Clean up workers that haven't sent a heartbeat recently"""
        try:
            with self.get_connection() as conn:
                conn.execute("BEGIN TRANSACTION")
                
                # Mark workers as stopped if no heartbeat for 30 seconds
                conn.execute("""
                    UPDATE workers 
                    SET status = 'stopped' 
                    WHERE status = 'running' 
                        AND datetime(last_heartbeat) < datetime('now', '-30 seconds')
                """)
                
                stale_count = conn.total_changes
                
                # Release jobs from stale workers
                conn.execute("""
                    UPDATE jobs 
                    SET state = 'pending', 
                        worker_id = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE state = 'processing' 
                        AND worker_id IN (
                            SELECT id FROM workers 
                            WHERE status = 'stopped'
                        )
                """)
                
                conn.execute("COMMIT")
                return stale_count
        except Exception as e:
            logger.error(f"Failed to cleanup stale workers: {e}")
            return 0
