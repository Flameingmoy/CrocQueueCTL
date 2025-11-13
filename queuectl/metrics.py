"""Metrics and statistics module for CrocQueuectl"""

from datetime import datetime, timedelta
from typing import Dict, List, Any
import statistics


class MetricsCollector:
    """Collects and analyzes job execution metrics"""
    
    def __init__(self, db):
        self.db = db
    
    def get_execution_stats(self) -> Dict[str, Any]:
        """Get overall execution statistics"""
        with self.db.get_connection() as conn:
            # Get job counts by state
            cursor = conn.execute("""
                SELECT 
                    state,
                    COUNT(*) as count,
                    AVG(attempts) as avg_attempts
                FROM jobs
                GROUP BY state
            """)
            stats_by_state = {row['state']: {
                'count': row['count'], 
                'avg_attempts': row['avg_attempts'] or 0
            } for row in cursor.fetchall()}
            
            # Get success rate (last 24 hours)
            cursor = conn.execute("""
                SELECT 
                    COUNT(CASE WHEN state = 'completed' THEN 1 END) as completed,
                    COUNT(*) as total
                FROM jobs
                WHERE created_at >= datetime('now', '-24 hours')
            """)
            row = cursor.fetchone()
            success_rate = (row['completed'] / row['total'] * 100) if row['total'] > 0 else 0
            
            # Get average processing time (for completed jobs)
            cursor = conn.execute("""
                SELECT 
                    AVG(CAST((julianday(updated_at) - julianday(created_at)) * 86400 AS REAL)) as avg_time
                FROM jobs
                WHERE state = 'completed'
                AND updated_at IS NOT NULL
            """)
            avg_processing_time = cursor.fetchone()['avg_time'] or 0
            
            # Get top failing commands
            cursor = conn.execute("""
                SELECT 
                    command,
                    COUNT(*) as fail_count,
                    MAX(error_message) as last_error
                FROM jobs
                WHERE state = 'dead'
                GROUP BY command
                ORDER BY fail_count DESC
                LIMIT 5
            """)
            top_failures = [dict(row) for row in cursor.fetchall()]
            
            return {
                'states': stats_by_state,
                'success_rate_24h': round(success_rate, 2),
                'avg_processing_time_seconds': round(avg_processing_time, 2),
                'top_failures': top_failures
            }
    
    def get_worker_stats(self) -> List[Dict[str, Any]]:
        """Get statistics about worker performance"""
        with self.db.get_connection() as conn:
            cursor = conn.execute("""
                SELECT 
                    w.id as worker_id,
                    w.status,
                    w.started_at,
                    w.last_heartbeat,
                    COUNT(j.id) as jobs_processed
                FROM workers w
                LEFT JOIN jobs j ON j.worker_id = w.id AND j.state = 'completed'
                GROUP BY w.id
                ORDER BY w.started_at DESC
                LIMIT 10
            """)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_throughput(self, minutes: int = 60) -> Dict[str, float]:
        """Calculate job throughput (jobs per minute)"""
        with self.db.get_connection() as conn:
            cursor = conn.execute(f"""
                SELECT 
                    COUNT(*) as jobs_completed,
                    MIN(created_at) as first_job,
                    MAX(updated_at) as last_job
                FROM jobs
                WHERE state = 'completed'
                AND updated_at >= datetime('now', '-{minutes} minutes')
            """)
            row = cursor.fetchone()
            
            if row and row['jobs_completed'] > 0:
                # Calculate actual time span
                first = datetime.fromisoformat(row['first_job'])
                last = datetime.fromisoformat(row['last_job'])
                time_span_minutes = (last - first).total_seconds() / 60
                
                if time_span_minutes > 0:
                    throughput = row['jobs_completed'] / time_span_minutes
                else:
                    throughput = row['jobs_completed']  # All processed instantly
                
                return {
                    'jobs_completed': row['jobs_completed'],
                    'time_span_minutes': round(time_span_minutes, 2),
                    'jobs_per_minute': round(throughput, 2)
                }
            
            return {
                'jobs_completed': 0,
                'time_span_minutes': 0,
                'jobs_per_minute': 0
            }
    
    def get_retry_statistics(self) -> Dict[str, Any]:
        """Get statistics about job retries"""
        with self.db.get_connection() as conn:
            cursor = conn.execute("""
                SELECT 
                    AVG(attempts) as avg_attempts,
                    MAX(attempts) as max_attempts,
                    COUNT(CASE WHEN attempts > 0 THEN 1 END) as jobs_with_retries,
                    COUNT(*) as total_jobs
                FROM jobs
                WHERE state IN ('completed', 'dead')
            """)
            row = cursor.fetchone()
            
            retry_rate = (row['jobs_with_retries'] / row['total_jobs'] * 100) if row['total_jobs'] > 0 else 0
            
            return {
                'avg_attempts': round(row['avg_attempts'] or 0, 2),
                'max_attempts': row['max_attempts'] or 0,
                'retry_rate': round(retry_rate, 2),
                'jobs_with_retries': row['jobs_with_retries'] or 0
            }
