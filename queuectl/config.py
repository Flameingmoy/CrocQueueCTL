"""Configuration management module"""

import os
from typing import Any, Optional


class Config:
    """Configuration manager for queuectl"""
    
    # Default configuration values
    DEFAULTS = {
        'max_retries': 3,
        'backoff_base': 2,
        'worker_heartbeat_interval': 5,  # seconds
        'worker_poll_interval': 1,  # seconds
        'db_path': os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "queuectl.db")
    }
    
    def __init__(self, db=None):
        self.db = db
        self._cache = {}
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value"""
        # Check cache first
        if key in self._cache:
            return self._cache[key]
        
        # Try database if available
        if self.db:
            value = self.db.get_config(key)
            if value is not None:
                # Convert string values to appropriate types
                if key in ['max_retries', 'backoff_base', 'worker_heartbeat_interval', 'worker_poll_interval']:
                    value = int(value)
                self._cache[key] = value
                return value
        
        # Fall back to defaults
        if key in self.DEFAULTS:
            return self.DEFAULTS[key]
        
        return default
    
    def set(self, key: str, value: Any) -> bool:
        """Set configuration value"""
        if self.db:
            # Convert value to string for database storage
            str_value = str(value)
            success = self.db.set_config(key, str_value)
            if success:
                self._cache[key] = value
            return success
        return False
    
    def get_all(self) -> dict:
        """Get all configuration values"""
        config = self.DEFAULTS.copy()
        
        # Override with database values if available
        if self.db:
            for key in self.DEFAULTS:
                db_value = self.db.get_config(key)
                if db_value is not None:
                    if key in ['max_retries', 'backoff_base', 'worker_heartbeat_interval', 'worker_poll_interval']:
                        config[key] = int(db_value)
                    else:
                        config[key] = db_value
        
        return config
