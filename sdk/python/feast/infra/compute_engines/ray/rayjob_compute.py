"""
RayJob-based Compute Engine for Feast

This module provides a RayJob CR-based compute engine that executes
Feast Ray DAGs as discrete Kubernetes RayJob CRs.
"""

import logging
from datetime import datetime
from typing import Sequence, Union

from feast import (
    BatchFeatureView,
    Entity,
    FeatureView,
    OnDemandFeatureView,
    StreamFeatureView,
)
from feast.infra.common.materialization_job import (
    MaterializationJob,
    MaterializationJobStatus,
    MaterializationTask,
)
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.base import ComputeEngine
from feast.infra.compute_engines.ray.config import RayComputeEngineConfig
from feast.infra.compute_engines.ray.dag_orchestrator import RayJobDAGOrchestrator
from feast.infra.compute_engines.ray.codeflare_orchestrator import CodeFlareDAGOrchestrator
from feast.infra.compute_engines.ray.feature_builder import RayFeatureBuilder
from feast.infra.compute_engines.ray.job import (
    RayDAGRetrievalJob,
    RayMaterializationJob,
)
from feast.infra.compute_engines.ray.utils import write_to_online_store
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.infra.registry.base_registry import BaseRegistry

logger = logging.getLogger(__name__)


class RayJobComputeEngine(ComputeEngine):
    """
    RayJob CR-based compute engine for distributed feature computation.
    
    This engine uses Kubernetes RayJob CRs to execute Feast Ray DAGs as
    discrete, isolated steps, providing better resource management,
    fault tolerance, and observability.
    """

    def __init__(
        self,
        offline_store,
        online_store,
        repo_config,
        **kwargs,
    ):
        super().__init__(
            offline_store=offline_store,
            online_store=online_store,
            repo_config=repo_config,
            **kwargs,
        )
        
        # Extract RayJob-specific configuration
        self.rayjob_config = self._extract_rayjob_config(repo_config)
        
        # Create DAG orchestrator based on configuration
        if self.rayjob_config.use_codeflare_sdk:
            logger.info("Using CodeFlare SDK for RayJob management")
            self.orchestrator = CodeFlareDAGOrchestrator(self.rayjob_config)
        else:
            logger.info("Using custom RayJob CR orchestrator")
            self.orchestrator = RayJobDAGOrchestrator(self.rayjob_config)
        
        logger.info("RayJob Compute Engine initialized")

    def _extract_rayjob_config(self, repo_config) -> RayComputeEngineConfig:
        """Extract RayJob configuration from repo config."""
        batch_engine_config = getattr(repo_config, 'batch_engine', {})
        
        if isinstance(batch_engine_config, dict):
            # Convert dict to RayComputeEngineConfig
            config_dict = batch_engine_config.copy()
            config_dict.setdefault('type', 'ray.engine')
            config_dict.setdefault('max_workers', 4)
            config_dict.setdefault('enable_optimization', True)
            
            return RayComputeEngineConfig(**config_dict)
        else:
            # Use existing config
            return batch_engine_config

    def get_historical_features(
        self, registry: BaseRegistry, task: HistoricalRetrievalTask
    ) -> RetrievalJob:
        """
        Get historical features using RayJob CR-based DAG execution.
        
        Args:
            registry: Feature registry
            task: Historical retrieval task
            
        Returns:
            RetrievalJob for historical features
        """
        if isinstance(task.entity_df, str):
            raise NotImplementedError(
                "SQL-based entity_df is not yet supported in RayJob DAG"
            )

        try:
            logger.info("Starting historical feature retrieval via RayJob CRs")
            
            # Build typed execution context
            context = self.get_execution_context(registry, task)

            # Construct Feature Builder and build execution plan
            builder = RayFeatureBuilder(registry, task.feature_view, task, self.rayjob_config)
            plan = builder.build()

            # Create RayJob-based retrieval job
            return RayJobRetrievalJob(
                plan=plan,
                context=context,
                config=self.repo_config,
                orchestrator=self.orchestrator,
                full_feature_names=task.full_feature_name,
                on_demand_feature_views=getattr(task, "on_demand_feature_views", None),
                feature_refs=getattr(task, "feature_refs", None),
            )

        except Exception as e:
            logger.error(f"Historical feature retrieval failed: {e}")
            return RayJobRetrievalJob(
                plan=None,
                context=None,
                config=self.repo_config,
                orchestrator=self.orchestrator,
                full_feature_names=task.full_feature_name,
                on_demand_feature_views=getattr(task, "on_demand_feature_views", None),
                feature_refs=getattr(task, "feature_refs", None),
                error=e,
            )

    def _materialize_from_offline_store(
        self,
        registry: BaseRegistry,
        feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        start_date: datetime,
        end_date: datetime,
        project: str,
    ) -> MaterializationJob:
        """
        Materialize features using RayJob CR-based DAG execution.
        
        Args:
            registry: Feature registry
            feature_view: Feature view to materialize
            start_date: Start date for materialization
            end_date: End date for materialization
            project: Project name
            
        Returns:
            MaterializationJob for feature materialization
        """
        try:
            logger.info(f"Starting materialization via RayJob CRs for {feature_view.name}")
            
            # Create materialization task
            task = MaterializationTask(
                feature_view=feature_view,
                start_date=start_date,
                end_date=end_date,
                project=project,
            )

            # Build execution context
            context = self.get_execution_context(registry, task)

            # Construct Feature Builder and build execution plan
            builder = RayFeatureBuilder(registry, feature_view, task, self.rayjob_config)
            plan = builder.build()

            # Create RayJob-based materialization job
            return RayJobMaterializationJob(
                plan=plan,
                context=context,
                config=self.repo_config,
                orchestrator=self.orchestrator,
                feature_view=feature_view,
                start_date=start_date,
                end_date=end_date,
                project=project,
            )

        except Exception as e:
            logger.error(f"Materialization failed: {e}")
            return RayJobMaterializationJob(
                plan=None,
                context=None,
                config=self.repo_config,
                orchestrator=self.orchestrator,
                feature_view=feature_view,
                start_date=start_date,
                end_date=end_date,
                project=project,
                error=e,
            )

    def get_execution_status(self) -> dict:
        """
        Get current execution status.
        
        Returns:
            Dictionary containing execution status information
        """
        return self.orchestrator.get_execution_status()


class RayJobRetrievalJob(RetrievalJob):
    """
    Retrieval job that executes via RayJob CRs.
    
    This class handles the execution of historical feature retrieval
    using discrete RayJob CRs for each DAG step.
    """

    def __init__(
        self,
        plan,
        context,
        config,
        orchestrator: RayJobDAGOrchestrator,
        full_feature_names: bool = False,
        on_demand_feature_views: Sequence[OnDemandFeatureView] = None,
        feature_refs: Sequence[str] = None,
        error: Exception = None,
    ):
        super().__init__()
        self._plan = plan
        self._context = context
        self._config = config
        self._orchestrator = orchestrator
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._feature_refs = feature_refs or []
        self._error = error
        self._result_dataset = None

    def to_df(self, validation_reference=None, timeout: Optional[int] = None):
        """Get results as pandas DataFrame."""
        if self._error:
            raise self._error
            
        result = self._ensure_executed()
        return result.data.to_pandas()

    def to_arrow(self, validation_reference=None, timeout: Optional[int] = None):
        """Get results as Arrow table."""
        if self._error:
            raise self._error
            
        result = self._ensure_executed()
        return result.data.to_arrow()

    def to_ray_dataset(self):
        """Get results as Ray Dataset."""
        if self._error:
            raise self._error
            
        result = self._ensure_executed()
        return result.data

    def _ensure_executed(self):
        """Ensure the execution plan has been executed via RayJob CRs."""
        if self._result_dataset is None and self._plan and self._context:
            try:
                # Execute DAG via RayJob CRs
                result = self._orchestrator.execute_dag(self._plan, self._context)
                self._result_dataset = result.data
                return result
            except Exception as e:
                self._error = e
                logger.error(f"RayJob DAG execution failed: {e}")
                raise
        elif self._result_dataset is None:
            raise ValueError("No execution plan available or execution failed")

        # Return a mock DAGValue for compatibility
        from feast.infra.compute_engines.dag.value import DAGValue, DAGFormat
        return DAGValue(data=self._result_dataset, format=DAGFormat.RAY)


class RayJobMaterializationJob(MaterializationJob):
    """
    Materialization job that executes via RayJob CRs.
    
    This class handles the execution of feature materialization
    using discrete RayJob CRs for each DAG step.
    """

    def __init__(
        self,
        plan,
        context,
        config,
        orchestrator: RayJobDAGOrchestrator,
        feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        start_date: datetime,
        end_date: datetime,
        project: str,
        error: Exception = None,
    ):
        super().__init__()
        self._plan = plan
        self._context = context
        self._config = config
        self._orchestrator = orchestrator
        self._feature_view = feature_view
        self._start_date = start_date
        self._end_date = end_date
        self._project = project
        self._error = error
        self._status = MaterializationJobStatus.PENDING

    def status(self) -> MaterializationJobStatus:
        """Get materialization job status."""
        if self._error:
            return MaterializationJobStatus.FAILED
            
        if self._status == MaterializationJobStatus.PENDING:
            # Check orchestrator status
            status_info = self._orchestrator.get_execution_status()
            if status_info["active_jobs"] == 0 and status_info["completed_steps"] > 0:
                self._status = MaterializationJobStatus.COMPLETED
            elif status_info["active_jobs"] > 0:
                self._status = MaterializationJobStatus.RUNNING
                
        return self._status

    def error(self) -> Optional[Exception]:
        """Get materialization job error."""
        return self._error

    def wait(self, timeout: Optional[int] = None):
        """Wait for materialization to complete."""
        if self._error:
            raise self._error
            
        # Execute DAG via RayJob CRs
        try:
            result = self._orchestrator.execute_dag(self._plan, self._context)
            self._status = MaterializationJobStatus.COMPLETED
        except Exception as e:
            self._error = e
            self._status = MaterializationJobStatus.FAILED
            raise
