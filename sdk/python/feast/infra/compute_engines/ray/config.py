"""Configuration for Ray compute engine."""

from datetime import timedelta
from typing import Any, Dict, Literal, Optional

from pydantic import StrictStr

from feast.repo_config import FeastConfigBaseModel


class RayComputeEngineConfig(FeastConfigBaseModel):
    """Configuration for Ray Compute Engine."""

    type: Literal["ray.engine", "rayjob.engine"] = "ray.engine"
    """Ray Compute Engine type selector"""

    ray_address: Optional[str] = None
    """Ray cluster address. If None, uses local Ray cluster."""

    staging_location: Optional[StrictStr] = None
    """Remote path for batch materialization jobs"""

    # Ray-specific performance configurations
    broadcast_join_threshold_mb: int = 100
    """Threshold for using broadcast joins (in MB)"""

    enable_distributed_joins: bool = True
    """Whether to enable distributed joins for large datasets"""

    max_parallelism_multiplier: int = 2
    """Multiplier for max parallelism based on available CPUs"""

    target_partition_size_mb: int = 64
    """Target partition size in MB"""

    window_size_for_joins: str = "1H"
    """Window size for windowed temporal joins"""

    ray_conf: Optional[Dict[str, Any]] = None
    """Ray configuration parameters"""

    # Additional configuration options
    max_workers: Optional[int] = None
    """Maximum number of Ray workers. If None, uses all available cores."""

    enable_optimization: bool = True
    """Enable automatic performance optimizations."""

    execution_timeout_seconds: Optional[int] = None
    """Timeout for job execution in seconds."""

    # CodeFlare SDK configuration
    namespace: str = "default"
    """Kubernetes namespace for RayJob CRs"""

    step_timeout_seconds: int = 3600
    """Timeout for individual DAG steps in seconds"""

    use_codeflare_sdk: bool = False
    """Whether to use CodeFlare SDK for RayJob management"""

    use_existing_cluster: bool = False
    """Whether to use an existing Ray cluster (requires CodeFlare SDK)"""

    existing_cluster_name: Optional[str] = None
    """Name of existing Ray cluster to use"""

    cluster_lifecycle_mode: str = "per_job"
    """Cluster lifecycle mode: 'per_job' (single cluster) or 'per_step' (separate clusters)"""

    @property
    def window_size_timedelta(self) -> timedelta:
        """Convert window size string to timedelta."""
        if self.window_size_for_joins.endswith("H"):
            hours = int(self.window_size_for_joins[:-1])
            return timedelta(hours=hours)
        elif self.window_size_for_joins.endswith("min"):
            minutes = int(self.window_size_for_joins[:-3])
            return timedelta(minutes=minutes)
        elif self.window_size_for_joins.endswith("s"):
            seconds = int(self.window_size_for_joins[:-1])
            return timedelta(seconds=seconds)
        else:
            # Default to 1 hour
            return timedelta(hours=1)
