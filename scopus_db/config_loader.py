"""
Configuration Loader for Scopus Database Builder

Handles loading and validating configuration from config.json file with fallbacks
to environment variables and sensible defaults.
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional


class ConfigurationError(Exception):
    """Exception raised for configuration-related errors."""
    pass


class ConfigLoader:
    """
    Configuration loader with hierarchical settings precedence:
    1. config.json file (highest priority)
    2. Environment variables
    3. Default values (lowest priority)
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration loader.
        
        Args:
            config_path: Path to config.json file (defaults to project root)
        """
        if config_path:
            self.config_path = Path(config_path)
        else:
            # Look for config.json in project root
            project_root = Path(__file__).parent.parent
            self.config_path = project_root / "config.json"
        
        self.config = self._load_configuration()
        
    def _load_configuration(self) -> Dict[str, Any]:
        """Load configuration from file with environment variable and default fallbacks."""
        
        # Start with default configuration
        config = self._get_default_config()
        
        # Override with config file if it exists
        if self.config_path.exists():
            try:
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    file_config = json.load(f)
                config = self._merge_configs(config, file_config)
                print(f"✅ Configuration loaded from: {self.config_path}")
            except json.JSONDecodeError as e:
                raise ConfigurationError(f"Invalid JSON in config file {self.config_path}: {e}")
            except Exception as e:
                raise ConfigurationError(f"Error reading config file {self.config_path}: {e}")
        else:
            print(f"ℹ️  No config file found at {self.config_path}, using defaults + environment variables")
        
        # Override with environment variables (highest priority)
        config = self._apply_environment_overrides(config)
        
        # Validate configuration
        self._validate_config(config)
        
        return config
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration values."""
        return {
            "crossref": {
                "enabled": False,
                "email": None,
                "skip_confirmation": False,
                "rate_limit_requests_per_second": 45,
                "confidence_thresholds": {
                    "phase1_pubmed": 0.8,
                    "phase2a_journal": 0.75,
                    "phase2b_title": 0.65
                },
                "timeout_seconds": 30,
                "retry_attempts": 3
            },
            "data_quality": {
                "filtering_enabled": True,
                "generate_reports": True,
                "export_excluded_records": True,
                "quality_criteria": {
                    "require_authors": True,
                    "require_author_ids": True,
                    "require_title": True,
                    "require_year": True,
                    "require_doi": False,
                    "require_affiliations": True,
                    "require_abstract": True
                }
            },
            "database": {
                "include_analytics_tables": True,
                "create_indexes": True,
                "normalize_entities": True,
                "compute_collaborations": True,
                "compute_keyword_cooccurrence": True,
                "include_recovery_metadata": True
            },
            "output": {
                "generate_html_report": True,
                "generate_csv_export": True,
                "generate_text_report": True,
                "generate_json_log": True,
                "verbose_logging": True,
                "timestamp_files": True
            },
            "performance": {
                "batch_size": 1000,
                "memory_limit_mb": 2048,
                "parallel_processing": False,
                "cache_api_responses": True
            },
            "file_handling": {
                "encoding": "utf-8-sig",
                "skip_empty_rows": True,
                "handle_malformed_csv": True,
                "backup_original_files": False
            }
        }
    
    def _merge_configs(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively merge configuration dictionaries."""
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = value
                
        return result
    
    def _apply_environment_overrides(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Apply environment variable overrides to configuration."""
        
        # CrossRef email (most common override)
        if os.environ.get('CROSSREF_EMAIL'):
            config['crossref']['email'] = os.environ['CROSSREF_EMAIL']
            config['crossref']['enabled'] = True
            config['crossref']['skip_confirmation'] = True
            print(f"🔗 CrossRef email set from environment: {config['crossref']['email']}")
        
        # Other environment overrides
        env_mappings = {
            'CROSSREF_ENABLED': ('crossref', 'enabled', self._parse_bool),
            'CROSSREF_SKIP_CONFIRMATION': ('crossref', 'skip_confirmation', self._parse_bool),
            'DATA_FILTERING_ENABLED': ('data_quality', 'filtering_enabled', self._parse_bool),
            'VERBOSE_LOGGING': ('output', 'verbose_logging', self._parse_bool),
            'BATCH_SIZE': ('performance', 'batch_size', int),
            'MEMORY_LIMIT_MB': ('performance', 'memory_limit_mb', int),
        }
        
        for env_var, (section, key, converter) in env_mappings.items():
            if os.environ.get(env_var):
                try:
                    config[section][key] = converter(os.environ[env_var])
                    print(f"🔧 Override from {env_var}: {section}.{key} = {config[section][key]}")
                except ValueError as e:
                    print(f"⚠️  Invalid value for {env_var}: {e}")
        
        return config
    
    def _parse_bool(self, value: str) -> bool:
        """Parse boolean value from string."""
        return value.lower() in ('true', '1', 'yes', 'on', 'enabled')
    
    def _validate_config(self, config: Dict[str, Any]) -> None:
        """Validate configuration values."""
        
        # Validate CrossRef email if enabled
        if config['crossref']['enabled']:
            email = config['crossref']['email']
            if not email:
                raise ConfigurationError("CrossRef is enabled but no email provided")
            if '@' not in email or '.' not in email.split('@')[1]:
                raise ConfigurationError(f"Invalid email format: {email}")
        
        # Validate numeric values
        if config['crossref']['rate_limit_requests_per_second'] <= 0:
            raise ConfigurationError("Rate limit must be positive")
        
        if config['performance']['batch_size'] <= 0:
            raise ConfigurationError("Batch size must be positive")
        
        if config['performance']['memory_limit_mb'] <= 0:
            raise ConfigurationError("Memory limit must be positive")
        
        # Validate confidence thresholds
        thresholds = config['crossref']['confidence_thresholds']
        for phase, threshold in thresholds.items():
            if not 0.0 <= threshold <= 1.0:
                raise ConfigurationError(f"Confidence threshold for {phase} must be between 0.0 and 1.0")
    
    # Convenient getter methods
    def get_crossref_config(self) -> Dict[str, Any]:
        """Get CrossRef configuration."""
        return self.config['crossref']
    
    def get_data_quality_config(self) -> Dict[str, Any]:
        """Get data quality configuration."""
        return self.config['data_quality']
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration."""
        return self.config['database']
    
    def get_output_config(self) -> Dict[str, Any]:
        """Get output configuration."""
        return self.config['output']
    
    def get_performance_config(self) -> Dict[str, Any]:
        """Get performance configuration."""
        return self.config['performance']
    
    def get_file_handling_config(self) -> Dict[str, Any]:
        """Get file handling configuration."""
        return self.config['file_handling']
    
    def is_crossref_enabled(self) -> bool:
        """Check if CrossRef recovery is enabled."""
        return self.config['crossref']['enabled'] and bool(self.config['crossref']['email'])
    
    def get_crossref_email(self) -> Optional[str]:
        """Get CrossRef email address."""
        return self.config['crossref']['email']
    
    def print_configuration_summary(self) -> None:
        """Print a summary of current configuration."""
        print("\n📋 CONFIGURATION SUMMARY")
        print("=" * 50)
        
        # CrossRef settings
        crossref = self.config['crossref']
        print(f"🔗 CrossRef Recovery: {'✅ Enabled' if self.is_crossref_enabled() else '❌ Disabled'}")
        if self.is_crossref_enabled():
            print(f"   📧 Email: {crossref['email']}")
            print(f"   🤖 Auto-confirm: {crossref['skip_confirmation']}")
            print(f"   ⚡ Rate limit: {crossref['rate_limit_requests_per_second']} req/sec")
        
        # Data quality settings
        dq = self.config['data_quality']
        print(f"🔍 Data Quality: {'✅ Enabled' if dq['filtering_enabled'] else '❌ Disabled'}")
        required_fields = [k.replace('require_', '') for k, v in dq['quality_criteria'].items() if v]
        print(f"   📋 Required fields: {', '.join(required_fields)}")
        
        # Output settings
        output = self.config['output']
        enabled_outputs = [k.replace('generate_', '').replace('_', ' ') for k, v in output.items() 
                          if k.startswith('generate_') and v]
        print(f"📊 Output formats: {', '.join(enabled_outputs)}")
        
        # Performance settings
        perf = self.config['performance']
        print(f"⚡ Performance: Batch size={perf['batch_size']}, Memory={perf['memory_limit_mb']}MB")
        
        print("=" * 50)


# Global configuration instance
_config_instance = None

def get_config(config_path: Optional[str] = None) -> ConfigLoader:
    """Get global configuration instance (singleton pattern)."""
    global _config_instance
    if _config_instance is None:
        _config_instance = ConfigLoader(config_path)
    return _config_instance

def reload_config(config_path: Optional[str] = None) -> ConfigLoader:
    """Reload configuration from file."""
    global _config_instance
    _config_instance = ConfigLoader(config_path)
    return _config_instance