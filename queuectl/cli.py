"""CLI interface for queuectl"""

import click
import json
import logging
import sys
from tabulate import tabulate
from datetime import datetime

from .database import Database
from .job import Job
from .worker import WorkerManager
from .config import Config
from .metrics import MetricsCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


CROC_BANNER = r"""
╔════════════════════════════════════════════╗
║                                            ║
║   🐊 CrocQueuectl - Job Queue System       ║
║                                            ║
╚════════════════════════════════════════════╝
"""

@click.group()
@click.version_option(version='1.0.0', prog_name='CrocQueuectl')
@click.pass_context
def cli(ctx):
    """CrocQueuectl - CLI-based background job queue system
    
    A production-grade job queue with worker processes and retry logic.
    """
    ctx.ensure_object(dict)
    ctx.obj['db'] = Database()
    ctx.obj['config'] = Config(ctx.obj['db'])


@cli.command()
@click.argument('job_json')
@click.option('--priority', '-p', type=click.IntRange(1, 10), default=5,
              help='Job priority (1=low, 5=normal, 10=high)')
@click.option('--run-at', type=str, help='Schedule job at specific time (ISO format: YYYY-MM-DD HH:MM:SS)')
@click.option('--delay', type=str, help='Delay job execution (e.g., 30s, 5m, 1h)')
@click.pass_context
def enqueue(ctx, job_json, priority, run_at, delay):
    """Add a job to the queue
    
    Examples:
        queuectl enqueue '{"id":"job1","command":"echo hello"}'
        queuectl enqueue '{"id":"job2","command":"important"}' --priority 10
        queuectl enqueue '{"id":"job3","command":"later"}' --delay 30m
        queuectl enqueue '{"id":"job4","command":"scheduled"}' --run-at "2025-11-12 15:00:00"
    """
    db = ctx.obj['db']
    
    try:
        # Parse and validate job data
        job = Job.from_json(job_json)
        
        # Get default config values if not specified
        config = ctx.obj['config']
        job_data = job.to_dict()
        if 'max_retries' not in job_data:
            job_data['max_retries'] = config.get('max_retries')
        if 'backoff_base' not in job_data:
            job_data['backoff_base'] = config.get('backoff_base')
        
        # Add priority to job data
        job_data['priority'] = priority
        
        # Handle scheduling options
        from datetime import datetime, timedelta
        import re
        
        if run_at and delay:
            click.echo("[ERROR] Cannot specify both --run-at and --delay", err=True)
            sys.exit(1)
        
        if run_at:
            # Parse absolute time
            try:
                scheduled_time = datetime.fromisoformat(run_at)
                job_data['run_at'] = scheduled_time.strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                click.echo(f"[ERROR] Invalid date format. Use YYYY-MM-DD HH:MM:SS", err=True)
                sys.exit(1)
        elif delay:
            # Parse relative delay (e.g., 30s, 5m, 1h, 1d)
            match = re.match(r'(\d+)([smhd])', delay.lower())
            if not match:
                click.echo(f"[ERROR] Invalid delay format. Use format like 30s, 5m, 1h, 1d", err=True)
                sys.exit(1)
            
            amount, unit = match.groups()
            amount = int(amount)
            
            if unit == 's':
                delta = timedelta(seconds=amount)
            elif unit == 'm':
                delta = timedelta(minutes=amount)
            elif unit == 'h':
                delta = timedelta(hours=amount)
            elif unit == 'd':
                delta = timedelta(days=amount)
            
            scheduled_time = datetime.utcnow() + delta
            job_data['run_at'] = scheduled_time.strftime('%Y-%m-%d %H:%M:%S')
        
        # Enqueue the job
        if db.enqueue_job(job_data):
            priority_text = " (priority: high)" if priority > 7 else " (priority: low)" if priority < 4 else ""
            schedule_text = ""
            if job_data.get('run_at'):
                schedule_text = f" scheduled for {job_data['run_at']}"
            click.echo(f"[OK] Job '{job.id}' enqueued successfully{priority_text}{schedule_text}")
        else:
            click.echo(f"[ERROR] Failed to enqueue job '{job.id}' (may already exist)", err=True)
            sys.exit(1)
            
    except ValueError as e:
        click.echo(f"[ERROR] Invalid job data: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"[ERROR] Error enqueueing job: {e}", err=True)
        sys.exit(1)


@cli.group()
def worker():
    """Worker management commands"""
    pass


@worker.command('start')
@click.option('--count', '-c', default=1, help='Number of workers to start')
@click.option('--daemon', '-d', is_flag=True, help='Run workers in background (daemon mode)')
@click.pass_context
def worker_start(ctx, count, daemon):
    """Start worker process(es)
    
    Example:
        queuectl worker start --count 3
        queuectl worker start --daemon --count 3  # Non-blocking
    """
    db = ctx.obj['db']
    
    try:
        manager = WorkerManager(db.db_path)
        started_count = manager.start_workers(count, daemon=daemon)
        click.echo(f"[OK] Started {started_count} worker(s)")
        
        if daemon:
            # Daemon mode: return immediately
            click.echo("Workers running in background (daemon mode)")
            click.echo("Use 'queuectl worker stop' to stop them")
            return
        else:
            # Interactive mode: wait for Ctrl+C
            click.echo("Press Ctrl+C to stop workers...")
            try:
                import signal
                signal.pause()
            except KeyboardInterrupt:
                click.echo("\nStopping workers...")
                manager.stop_workers()
                click.echo("[OK] All workers stopped")
            
    except Exception as e:
        click.echo(f"[ERROR] Error starting workers: {e}", err=True)
        sys.exit(1)


@worker.command('stop')
@click.pass_context
def worker_stop(ctx):
    """Stop all worker processes gracefully
    
    Example:
        queuectl worker stop
    """
    db = ctx.obj['db']
    
    try:
        # Get all running workers from database
        workers = db.get_active_workers()
        stopped_count = 0
        
        for worker in workers:
            try:
                # Try to kill the process by PID
                pid = worker.get('pid')
                if pid:
                    import os
                    import signal
                    os.kill(pid, signal.SIGTERM)
                    stopped_count += 1
                    # Mark worker as stopped in database
                    db.mark_worker_stopped(worker['id'])
            except ProcessLookupError:
                # Process already dead, just mark as stopped
                db.mark_worker_stopped(worker['id'])
            except Exception as e:
                click.echo(f"Warning: Could not stop worker {worker['id']}: {e}", err=True)
        
        click.echo(f"[OK] Stopped {stopped_count} worker(s)")
        
        # Clean up stale workers
        stale_count = db.cleanup_stale_workers()
        if stale_count > 0:
            click.echo(f"[OK] Cleaned up {stale_count} stale workers")
        
    except Exception as e:
        click.echo(f"[ERROR] Error stopping workers: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def status(ctx):
    """Show job summary and active workers
    
    Example:
        queuectl status
    """
    db = ctx.obj['db']
    
    try:
        # Get job summary
        summary = db.get_status_summary()
        
        click.echo("\nJob Queue Status")
        click.echo("=" * 40)
        
        states = ['pending', 'processing', 'completed', 'failed', 'dead']
        for state in states:
            count = summary.get(state, 0)
            click.echo(f"{state.capitalize():12} {count:5d} jobs")
        
        click.echo(f"\nTotal:           {sum(summary.values()):5d} jobs")
        
        # Get active workers
        workers = db.get_active_workers()
        
        click.echo("\nActive Workers")
        click.echo("=" * 40)
        
        if workers:
            table_data = []
            for worker in workers:
                table_data.append([
                    worker['id'][:20],
                    worker['pid'],
                    worker.get('current_job_id', '-')[:20] if worker.get('current_job_id') else '-',
                    worker['started_at'][:19]
                ])
            
            click.echo(tabulate(
                table_data,
                headers=['Worker ID', 'PID', 'Current Job', 'Started At'],
                tablefmt='simple'
            ))
        else:
            click.echo("No active workers")
        
        click.echo()
        
    except Exception as e:
        click.echo(f"[ERROR] Error getting status: {e}", err=True)
        sys.exit(1)


@cli.command('list')
@click.option('--state', '-s', help='Filter by job state (pending/processing/completed/failed/dead)')
@click.option('--limit', '-l', default=20, help='Maximum number of jobs to display')
@click.pass_context
def list_jobs(ctx, state, limit):
    """List jobs, optionally filtered by state
    
    Example:
        queuectl list --state pending
    """
    db = ctx.obj['db']
    
    try:
        jobs = db.list_jobs(state)
        
        if not jobs:
            if state:
                click.echo(f"No jobs with state '{state}'")
            else:
                click.echo("No jobs in queue")
            return
        
        # Limit number of jobs displayed
        display_jobs = jobs[:limit]
        
        # Prepare table data
        headers = ['Job ID', 'State', 'Priority', 'Attempts', 'Command', 'Created At']
        table_data = []
        
        for job in display_jobs:
            # Truncate long commands
            cmd = job['command'][:40] + ('...' if len(job['command']) > 40 else '')
            priority = job.get('priority', 5)
            table_data.append([
                job['id'],
                job['state'],
                priority,
                job.get('attempts', 0),
                cmd,
                job['created_at'][:19]  # Remove microseconds
            ])
        
        # Display table
        click.echo(tabulate(
            table_data,
            headers=headers,
            tablefmt='simple'
        ))
        
        if len(jobs) > limit:
            click.echo(f"\n(Showing {limit} of {len(jobs)} jobs)")
        
    except Exception as e:
        click.echo(f"[ERROR] Error listing jobs: {e}", err=True)
        sys.exit(1)


@cli.group()
def dlq():
    """Dead Letter Queue management"""
    pass


@dlq.command('list')
@click.option('--limit', '-l', default=20, help='Maximum number of jobs to display')
@click.pass_context
def dlq_list(ctx, limit):
    """List jobs in the Dead Letter Queue
    
    Example:
        queuectl dlq list
    """
    db = ctx.obj['db']
    
    try:
        jobs = db.get_dlq_jobs()
        
        if not jobs:
            click.echo("No jobs in Dead Letter Queue")
            return
        
        # Limit number of jobs displayed
        display_jobs = jobs[:limit]
        
        # Prepare table data
        table_data = []
        for job in display_jobs:
            error_msg = job.get('error_message', '-')[:40]
            if len(error_msg) == 40:
                error_msg += '...'
            
            table_data.append([
                job['id'][:30],
                job.get('attempts', 0),
                job['command'][:30] + ('...' if len(job['command']) > 30 else ''),
                error_msg,
                job['updated_at'][:19]
            ])
        
        # Display table
        click.echo("\nDead Letter Queue")
        click.echo("=" * 80)
        click.echo(tabulate(
            table_data,
            headers=['Job ID', 'Attempts', 'Command', 'Error', 'Failed At'],
            tablefmt='simple'
        ))
        
        if len(jobs) > limit:
            click.echo(f"\n(Showing {limit} of {len(jobs)} jobs)")
        
    except Exception as e:
        click.echo(f"[ERROR] Error listing DLQ jobs: {e}", err=True)
        sys.exit(1)


@dlq.command('retry')
@click.argument('job_id')
@click.pass_context
def dlq_retry(ctx, job_id):
    """Retry a job from the Dead Letter Queue
    
    Example:
        queuectl dlq retry job1
    """
    db = ctx.obj['db']
    
    try:
        # Check if job exists in DLQ
        job = db.get_job(job_id)
        if not job:
            click.echo(f"[ERROR] Job '{job_id}' not found", err=True)
            sys.exit(1)
        
        if job['state'] != 'dead':
            click.echo(f"[ERROR] Job '{job_id}' is not in the Dead Letter Queue (state: {job['state']})", err=True)
            sys.exit(1)
        
        # Retry the job
        if db.retry_dlq_job(job_id):
            click.echo(f"[OK] Job '{job_id}' moved back to pending queue for retry")
        else:
            click.echo(f"[ERROR] Failed to retry job '{job_id}'", err=True)
            sys.exit(1)
            
    except Exception as e:
        click.echo(f"[ERROR] Error retrying DLQ job: {e}", err=True)
        sys.exit(1)


@cli.group()
def config():
    """Configuration management"""
    pass


@config.command('get')
@click.argument('key', required=False)
@click.pass_context
def config_get(ctx, key):
    """Get configuration value(s)
    
    Example:
        queuectl config get max-retries
        queuectl config get  # Show all config
    """
    config = ctx.obj['config']
    
    try:
        if key:
            # Convert key format (max-retries -> max_retries)
            config_key = key.replace('-', '_')
            value = config.get(config_key)
            
            if value is not None:
                click.echo(f"{key}: {value}")
            else:
                click.echo(f"[ERROR] Unknown configuration key: {key}", err=True)
                sys.exit(1)
        else:
            # Show all configuration
            all_config = config.get_all()
            click.echo("\nConfiguration")
            click.echo("=" * 40)
            for k, v in all_config.items():
                display_key = k.replace('_', '-')
                click.echo(f"{display_key:25} {v}")
            click.echo()
            
    except Exception as e:
        click.echo(f"[ERROR] Error getting configuration: {e}", err=True)
        sys.exit(1)


@config.command('set')
@click.argument('key')
@click.argument('value')
@click.pass_context
def config_set(ctx, key, value):
    """Set a configuration value
    
    Example:
        queuectl config set max-retries 5
        queuectl config set backoff-base 3
    """
    config = ctx.obj['config']
    
    try:
        # Convert key format (max-retries -> max_retries)
        config_key = key.replace('-', '_')
        
        # Validate known keys
        if config_key not in ['max_retries', 'backoff_base']:
            click.echo(f"[ERROR] Unknown configuration key: {key}", err=True)
            click.echo("Valid keys: max-retries, backoff-base")
            sys.exit(1)
        
        # Validate value is a positive integer
        try:
            int_value = int(value)
            if int_value <= 0:
                raise ValueError("Value must be positive")
        except ValueError:
            click.echo(f"[ERROR] Invalid value: {value} (must be a positive integer)", err=True)
            sys.exit(1)
        
        # Set the configuration
        if config.set(config_key, int_value):
            click.echo(f"[OK] Configuration updated: {key} = {int_value}")
        else:
            click.echo(f"[ERROR] Failed to update configuration", err=True)
            sys.exit(1)
            
    except Exception as e:
        click.echo(f"[ERROR] Error setting configuration: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--detailed', '-d', is_flag=True, help='Show detailed metrics')
@click.pass_context
def metrics(ctx, detailed):
    """Show execution metrics and statistics
    
    Example:
        queuectl metrics
        queuectl metrics --detailed
    """
    db = ctx.obj['db']
    collector = MetricsCollector(db)
    
    try:
        # Print banner
        click.echo("\nCrocQueuectl Metrics Dashboard")
        click.echo("=" * 50)
        
        # Get and display execution stats
        stats = collector.get_execution_stats()
        
        click.echo("\nExecution Statistics")
        click.echo("-" * 30)
        for state, data in stats['states'].items():
            click.echo(f"{state.capitalize():12} {data['count']:5d} jobs (avg {data['avg_attempts']:.1f} attempts)")
        
        click.echo(f"\nSuccess Rate (24h): {stats['success_rate_24h']}%")
        click.echo(f"Avg Processing Time: {stats['avg_processing_time_seconds']}s")
        
        # Show throughput
        throughput = collector.get_throughput(60)
        click.echo(f"\nThroughput (last hour)")
        click.echo("-" * 30)
        click.echo(f"Jobs completed: {throughput['jobs_completed']}")
        click.echo(f"Rate: {throughput['jobs_per_minute']} jobs/minute")
        
        if detailed:
            # Show retry statistics
            retry_stats = collector.get_retry_statistics()
            click.echo(f"\nRetry Statistics")
            click.echo("-" * 30)
            click.echo(f"Retry rate: {retry_stats['retry_rate']}%")
            click.echo(f"Avg attempts: {retry_stats['avg_attempts']}")
            click.echo(f"Max attempts seen: {retry_stats['max_attempts']}")
            
            # Show top failures
            if stats['top_failures']:
                click.echo(f"\nTop Failures")
                click.echo("-" * 30)
                for failure in stats['top_failures']:
                    cmd = failure['command'][:40] + ('...' if len(failure['command']) > 40 else '')
                    click.echo(f"  {failure['fail_count']:3d}x {cmd}")
                    if failure['last_error']:
                        error = failure['last_error'][:50] + ('...' if len(failure['last_error']) > 50 else '')
                        click.echo(f"       Last error: {error}")
            
            # Show worker stats
            worker_stats = collector.get_worker_stats()
            if worker_stats:
                click.echo(f"\nWorker Statistics")
                click.echo("-" * 30)
                table_data = []
                for worker in worker_stats[:5]:
                    table_data.append([
                        worker['worker_id'][:12],
                        worker['status'],
                        worker['jobs_processed'],
                        worker['started_at'][:19]
                    ])
                click.echo(tabulate(
                    table_data,
                    headers=['Worker ID', 'Status', 'Jobs', 'Started'],
                    tablefmt='simple'
                ))
        
        click.echo()
        
    except Exception as e:
        click.echo(f"✗ Error getting metrics: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    cli()
