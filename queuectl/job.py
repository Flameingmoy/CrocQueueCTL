"""Job execution and management module"""

import subprocess
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class Job:
    """Represents a job to be executed"""
    
    def __init__(self, data: Dict[str, Any]):
        self.id = data['id']
        self.command = data['command']
        self.state = data.get('state', 'pending')
        self.attempts = data.get('attempts', 0)
        self.max_retries = data.get('max_retries', 3)
        self.backoff_base = data.get('backoff_base', 2)
        self.created_at = data.get('created_at')
        self.updated_at = data.get('updated_at')
        self.error_message = data.get('error_message')
        self.output = data.get('output')
    
    def execute(self) -> tuple[bool, str, str]:
        """
        Execute the job command.
        Returns: (success, output, error_message)
        """
        logger.info(f"Executing job {self.id}: {self.command}")
        
        try:
            # Execute the command
            result = subprocess.run(
                self.command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=300  # 5-minute timeout
            )
            
            output = result.stdout + result.stderr
            
            if result.returncode == 0:
                logger.info(f"Job {self.id} completed successfully")
                return True, output, None
            else:
                error_msg = f"Command failed with exit code {result.returncode}"
                logger.warning(f"Job {self.id} failed: {error_msg}")
                return False, output, error_msg
                
        except subprocess.TimeoutExpired:
            error_msg = "Command timed out after 5 minutes"
            logger.error(f"Job {self.id} timed out")
            return False, "", error_msg
            
        except FileNotFoundError as e:
            error_msg = f"Command not found: {str(e)}"
            logger.error(f"Job {self.id} command not found: {e}")
            return False, "", error_msg
            
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(f"Job {self.id} unexpected error: {e}")
            return False, "", error_msg
    
    def should_retry(self) -> bool:
        """Check if the job should be retried"""
        return self.attempts < self.max_retries
    
    def calculate_next_retry_time(self) -> datetime:
        """Calculate when the job should be retried using exponential backoff"""
        delay_seconds = self.backoff_base ** (self.attempts + 1)
        # Use UTC for consistency with SQLite datetime('now')
        return datetime.utcnow() + timedelta(seconds=delay_seconds)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert job to dictionary"""
        return {
            'id': self.id,
            'command': self.command,
            'state': self.state,
            'attempts': self.attempts,
            'max_retries': self.max_retries,
            'backoff_base': self.backoff_base,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'error_message': self.error_message,
            'output': self.output
        }
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Job':
        """Create a Job from JSON string"""
        try:
            data = json.loads(json_str)
            # Validate required fields
            if 'id' not in data:
                raise ValueError("Job must have an 'id' field")
            if 'command' not in data:
                raise ValueError("Job must have a 'command' field")
            return cls(data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON: {e}")


class JobProcessor:
    """Handles job processing logic"""
    
    def __init__(self, db):
        self.db = db
    
    def process_job(self, job_data: Dict[str, Any], worker_id: str) -> bool:
        """
        Process a single job, handling execution and state updates.
        Returns True if job completed successfully, False otherwise.
        """
        job = Job(job_data)
        
        # Update worker heartbeat
        self.db.update_worker_heartbeat(worker_id, job.id)
        
        # Execute the job
        success, output, error_msg = job.execute()
        
        # Update worker heartbeat after execution
        self.db.update_worker_heartbeat(worker_id, None)
        
        if success:
            # Job completed successfully
            self.db.update_job_state(job.id, 'completed', output=output)
            logger.info(f"Job {job.id} completed successfully")
            return True
        else:
            # Job failed - check if should retry
            if job.should_retry():
                # Schedule retry with exponential backoff
                next_retry = job.calculate_next_retry_time()
                # Keep in pending state with next_retry_at set
                # Format timestamp for SQLite (YYYY-MM-DD HH:MM:SS)
                self.db.update_job_state(
                    job.id, 
                    'pending',
                    error_message=error_msg,
                    output=output,
                    next_retry_at=next_retry.strftime('%Y-%m-%d %H:%M:%S')
                )
                logger.info(f"Job {job.id} failed (attempt {job.attempts + 1}/{job.max_retries}), will retry at {next_retry}")
            else:
                # Max retries exceeded, move to DLQ
                self.db.move_to_dlq(job.id, error_msg)
                logger.warning(f"Job {job.id} moved to DLQ after {job.attempts + 1} attempts")
            
            return False
