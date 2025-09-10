"""
CodeFlare SDK Integration for Feast RayJob DAG Execution

This module provides CodeFlare SDK integration for executing Feast Ray DAGs
using either existing Ray clusters or lifecycle-managed clusters.
"""

import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from feast.infra.compute_engines.dag.context import ExecutionContext
from feast.infra.compute_engines.dag.plan import ExecutionPlan
from feast.infra.compute_engines.dag.value import DAGValue
from feast.infra.compute_engines.ray.config import RayComputeEngineConfig
from feast.infra.compute_engines.ray.dag_serializer import DAGStepSerializer

logger = logging.getLogger(__name__)


class CodeFlareDAGOrchestrator:
    """
    Orchestrates DAG execution via CodeFlare SDK with flexible cluster management.
    
    This class provides two execution modes:
    1. Existing Cluster Mode: Submit jobs to an existing Ray cluster
    2. Lifecycle Mode: Let CodeFlare manage cluster lifecycle per job
    """

    def __init__(self, config: RayComputeEngineConfig):
        self.config = config
        self.serializer = DAGStepSerializer(config)
        self.active_jobs = {}
        self.step_results = {}
        self.job_timeout = config.step_timeout_seconds
        
        # CodeFlare SDK configuration
        self.use_existing_cluster = getattr(config, 'use_existing_cluster', False)
        self.existing_cluster_name = getattr(config, 'existing_cluster_name', None)
        self.cluster_lifecycle_mode = getattr(config, 'cluster_lifecycle_mode', 'per_job')
        
        # Initialize CodeFlare SDK
        self._init_codeflare_sdk()

    def _init_codeflare_sdk(self):
        """Initialize CodeFlare SDK."""
        try:
            import codeflare_sdk
            self.codeflare = codeflare_sdk
            logger.info("CodeFlare SDK initialized successfully")
        except ImportError:
            logger.error("CodeFlare SDK not installed. Please install with: pip install codeflare-sdk")
            raise ImportError("CodeFlare SDK is required for this functionality")

    def execute_dag(
        self, 
        execution_plan: ExecutionPlan, 
        context: ExecutionContext
    ) -> DAGValue:
        """
        Execute DAG using CodeFlare SDK with flexible cluster management.
        
        Args:
            execution_plan: The ExecutionPlan to execute
            context: The ExecutionContext containing runtime information
            
        Returns:
            Final DAGValue result
        """
        logger.info(f"Starting DAG execution with CodeFlare SDK")
        logger.info(f"Cluster mode: {'existing' if self.use_existing_cluster else 'lifecycle'}")
        
        try:
            # Serialize execution plan into steps
            serialized_plan = self.serializer.serialize_execution_plan(execution_plan, context)
            steps = serialized_plan["steps"]
            dependencies = serialized_plan["dependencies"]
            
            logger.info(f"Serialized DAG into {len(steps)} discrete steps")
            
            if self.use_existing_cluster:
                return self._execute_with_existing_cluster(steps, dependencies)
            else:
                return self._execute_with_lifecycle_cluster(steps, dependencies)
                
        except Exception as e:
            logger.error(f"DAG execution failed: {e}")
            self._cleanup_active_jobs()
            raise

    def _execute_with_existing_cluster(self, steps: List[Dict], dependencies: Dict) -> DAGValue:
        """
        Execute DAG steps on an existing Ray cluster using RayJob CRs.
        
        Args:
            steps: List of serialized step configurations
            dependencies: Step dependency mapping
            
        Returns:
            Final DAGValue result
        """
        logger.info(f"Executing DAG on existing cluster via RayJob CRs: {self.existing_cluster_name}")
        
        # Connect to existing cluster
        cluster = self._connect_to_existing_cluster()
        
        try:
            # Execute steps in dependency order
            for step in steps:
                step_id = step["step_id"]
                logger.info(f"Executing step: {step_id}")
                
                # Wait for dependencies to complete
                self._wait_for_dependencies(step_id, dependencies[step_id])
                
                # Submit RayJob CR to existing cluster
                job_name = f"feast-dag-step-{step_id}-{int(time.time())}"
                self._submit_rayjob_cr_to_existing_cluster(cluster, job_name, step)
                
                # Monitor step completion
                result = self._monitor_step_completion(job_name, step_id)
                
                # Store result for next steps
                self.step_results[step_id] = result
                
            # Collect final result
            final_step = steps[-1]
            final_result = self.step_results[final_step["step_id"]]
            
            logger.info("DAG execution completed successfully on existing cluster via RayJob CRs")
            return final_result
            
        finally:
            # Note: We don't shutdown the existing cluster
            logger.info("DAG execution completed, existing cluster remains active")

    def _execute_with_lifecycle_cluster(self, steps: List[Dict], dependencies: Dict) -> DAGValue:
        """
        Execute DAG steps with CodeFlare-managed cluster lifecycle.
        
        Args:
            steps: List of serialized step configurations
            dependencies: Step dependency mapping
            
        Returns:
            Final DAGValue result
        """
        logger.info("Executing DAG with CodeFlare-managed cluster lifecycle")
        
        if self.cluster_lifecycle_mode == 'per_job':
            # Create one cluster for the entire DAG execution
            return self._execute_with_single_lifecycle_cluster(steps, dependencies)
        elif self.cluster_lifecycle_mode == 'per_step':
            # Create separate clusters for each step
            return self._execute_with_per_step_clusters(steps, dependencies)
        else:
            raise ValueError(f"Unknown cluster lifecycle mode: {self.cluster_lifecycle_mode}")

    def _execute_with_single_lifecycle_cluster(self, steps: List[Dict], dependencies: Dict) -> DAGValue:
        """
        Execute all DAG steps on a single lifecycle-managed cluster.
        
        Args:
            steps: List of serialized step configurations
            dependencies: Step dependency mapping
            
        Returns:
            Final DAGValue result
        """
        logger.info("Creating single lifecycle-managed cluster for entire DAG")
        
        # Create cluster for the entire DAG
        cluster_name = f"feast-dag-cluster-{int(time.time())}"
        cluster = self._create_lifecycle_cluster(cluster_name)
        
        try:
            # Execute steps in dependency order on the same cluster
            for step in steps:
                step_id = step["step_id"]
                logger.info(f"Executing step: {step_id}")
                
                # Wait for dependencies to complete
                self._wait_for_dependencies(step_id, dependencies[step_id])
                
                # Submit job to the same cluster
                job_name = f"feast-dag-step-{step_id}-{int(time.time())}"
                self._submit_to_lifecycle_cluster(cluster, job_name, step)
                
                # Monitor step completion
                result = self._monitor_step_completion(job_name, step_id)
                
                # Store result for next steps
                self.step_results[step_id] = result
                
            # Collect final result
            final_step = steps[-1]
            final_result = self.step_results[final_step["step_id"]]
            
            logger.info("DAG execution completed successfully on lifecycle cluster")
            return final_result
            
        finally:
            # Cleanup cluster
            self._cleanup_lifecycle_cluster(cluster)
            logger.info("Lifecycle cluster cleaned up")

    def _execute_with_per_step_clusters(self, steps: List[Dict], dependencies: Dict) -> DAGValue:
        """
        Execute each DAG step on separate lifecycle-managed clusters.
        
        Args:
            steps: List of serialized step configurations
            dependencies: Step dependency mapping
            
        Returns:
            Final DAGValue result
        """
        logger.info("Creating separate lifecycle-managed clusters for each step")
        
        # Execute steps in dependency order, each on its own cluster
        for step in steps:
            step_id = step["step_id"]
            logger.info(f"Executing step: {step_id}")
            
            # Wait for dependencies to complete
            self._wait_for_dependencies(step_id, dependencies[step_id])
            
            # Create cluster for this step
            cluster_name = f"feast-step-cluster-{step_id}-{int(time.time())}"
            cluster = self._create_lifecycle_cluster(cluster_name)
            
            try:
                # Submit job to step-specific cluster
                job_name = f"feast-dag-step-{step_id}-{int(time.time())}"
                self._submit_to_lifecycle_cluster(cluster, job_name, step)
                
                # Monitor step completion
                result = self._monitor_step_completion(job_name, step_id)
                
                # Store result for next steps
                self.step_results[step_id] = result
                
            finally:
                # Cleanup step-specific cluster
                self._cleanup_lifecycle_cluster(cluster)
                logger.info(f"Step cluster for {step_id} cleaned up")
                
        # Collect final result
        final_step = steps[-1]
        final_result = self.step_results[final_step["step_id"]]
        
        logger.info("DAG execution completed successfully with per-step clusters")
        return final_result

    def _connect_to_existing_cluster(self):
        """Connect to an existing Ray cluster."""
        if not self.existing_cluster_name:
            raise ValueError("existing_cluster_name must be specified for existing cluster mode")
            
        # Use CodeFlare SDK to connect to existing cluster
        cluster = self.codeflare.connect_cluster(self.existing_cluster_name)
        logger.info(f"Connected to existing cluster: {self.existing_cluster_name}")
        return cluster

    def _create_lifecycle_cluster(self, cluster_name: str):
        """Create a new lifecycle-managed cluster."""
        logger.info(f"Creating lifecycle cluster: {cluster_name}")
        
        # Use CodeFlare SDK's Cluster class directly
        cluster = self.codeflare.Cluster(
            name=cluster_name,
            namespace=getattr(self.config, 'namespace', 'default'),
            # CodeFlare SDK handles resource allocation, image selection, and other configurations automatically!
        )
        
        logger.info(f"Created lifecycle cluster: {cluster_name}")
        return cluster

    def _submit_rayjob_cr_to_existing_cluster(self, cluster, job_name: str, step_config: Dict[str, Any]):
        """Submit RayJob CR to existing cluster using CodeFlare SDK."""
        # Use CodeFlare SDK's RayJob class directly
        rayjob = self.codeflare.RayJob(
            job_name=job_name,
            cluster_name=self.existing_cluster_name,
            entrypoint=self._create_entrypoint_script(step_config),
            runtime_env={
                "pip": ["feast", "ray[data]", "pandas", "pyarrow", "dill"],
                "env_vars": {
                    "FEAST_STEP_ID": step_config["step_id"],
                    "FEAST_STEP_CONFIG": json.dumps(step_config),
                }
            }
            # CodeFlare SDK handles image, resources, and other configurations automatically!
        )
        
        # Submit RayJob CR via CodeFlare SDK
        job = rayjob.submit()
        
        self.active_jobs[job_name] = {
            "status": "submitted",
            "step_config": step_config,
            "job": job,
            "start_time": datetime.now(),
            "rayjob_cr": True,  # Mark as RayJob CR
        }
        
        logger.info(f"RayJob CR {job_name} submitted to existing cluster {self.existing_cluster_name}")

    def _submit_to_existing_cluster(self, cluster, job_name: str, step_config: Dict[str, Any]):
        """Submit job to existing cluster (legacy method)."""
        # Use CodeFlare SDK to submit job to existing cluster
        job_config = {
            "name": job_name,
            "entrypoint": self._create_entrypoint_script(step_config),
            "runtimeEnv": {
                "pip": ["feast", "ray[data]", "pandas", "pyarrow", "dill"],
                "env_vars": {
                    "FEAST_STEP_ID": step_config["step_id"],
                    "FEAST_STEP_CONFIG": json.dumps(step_config),
                }
            }
        }
        
        job = cluster.submit_job(job_config)
        self.active_jobs[job_name] = {
            "status": "submitted",
            "step_config": step_config,
            "job": job,
            "start_time": datetime.now(),
        }
        
        logger.info(f"Job {job_name} submitted to existing cluster")

    def _submit_to_lifecycle_cluster(self, cluster, job_name: str, step_config: Dict[str, Any]):
        """Submit job to lifecycle-managed cluster."""
        # Use CodeFlare SDK's RayJob class directly
        rayjob = self.codeflare.RayJob(
            job_name=job_name,
            cluster_name=cluster.name,
            entrypoint=self._create_entrypoint_script(step_config),
            runtime_env={
                "pip": ["feast", "ray[data]", "pandas", "pyarrow", "dill"],
                "env_vars": {
                    "FEAST_STEP_ID": step_config["step_id"],
                    "FEAST_STEP_CONFIG": json.dumps(step_config),
                }
            }
            # CodeFlare SDK handles image, resources, and other configurations automatically!
        )
        
        # Submit RayJob CR via CodeFlare SDK
        job = rayjob.submit()
        
        self.active_jobs[job_name] = {
            "status": "submitted",
            "step_config": step_config,
            "job": job,
            "start_time": datetime.now(),
        }
        
        logger.info(f"Job {job_name} submitted to lifecycle cluster")

    def _create_entrypoint_script(self, step_config: Dict[str, Any]) -> str:
        """Create entrypoint script for job execution."""
        script = """
import os
import json
import sys
sys.path.insert(0, '/opt/feast')

from feast.infra.compute_engines.ray.dag_step_executor import RayJobDAGStepRunner

# Get step configuration from environment
step_config_json = os.environ.get('FEAST_STEP_CONFIG', '{}')
input_data_json = os.environ.get('FEAST_INPUT_DATA', None)

# Create and run step executor
runner = RayJobDAGStepRunner()
runner.run_step(step_config_json, input_data_json)
"""
        return script.replace('\n', '\\n').replace('"', '\\"')

    def _wait_for_dependencies(self, step_id: str, dependencies: List[str]):
        """Wait for step dependencies to complete."""
        if not dependencies:
            return
            
        logger.info(f"Waiting for dependencies of {step_id}: {dependencies}")
        
        timeout = self.job_timeout
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            all_complete = True
            for dep in dependencies:
                if dep not in self.step_results:
                    all_complete = False
                    break
                    
            if all_complete:
                logger.info(f"All dependencies for {step_id} completed")
                return
                
            time.sleep(5)  # Wait 5 seconds before checking again
            
        raise TimeoutError(f"Dependencies for {step_id} did not complete within timeout")

    def _monitor_step_completion(self, job_name: str, step_id: str) -> DAGValue:
        """Monitor step completion and retrieve results."""
        logger.info(f"Monitoring step completion: {step_id}")
        
        timeout = self.job_timeout
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if job_name in self.active_jobs:
                job_info = self.active_jobs[job_name]
                job = job_info["job"]
                
                # Check if this is a RayJob CR
                if job_info.get("rayjob_cr", False):
                    # Monitor RayJob CR status using CodeFlare SDK
                    status = job.get_rayjob_status()
                    
                    if status == "COMPLETED":
                        # Get RayJob CR result
                        result = job.get_rayjob_result()
                        job_info["status"] = "completed"
                        
                        logger.info(f"RayJob CR {step_id} completed successfully")
                        return self._create_result_from_job_output(result)
                        
                    elif status == "FAILED":
                        error = job.get_rayjob_error()
                        job_info["status"] = "failed"
                        raise RuntimeError(f"RayJob CR {step_id} failed: {error}")
                        
                else:
                    # Monitor regular job status using CodeFlare SDK
                    status = job.get_status()
                    
                    if status == "COMPLETED":
                        # Get job result
                        result = job.get_result()
                        job_info["status"] = "completed"
                        
                        logger.info(f"Step {step_id} completed successfully")
                        return self._create_result_from_job_output(result)
                        
                    elif status == "FAILED":
                        error = job.get_error()
                        job_info["status"] = "failed"
                        raise RuntimeError(f"Step {step_id} failed: {error}")
                    
            time.sleep(5)  # Check every 5 seconds
            
        raise TimeoutError(f"Step {step_id} did not complete within timeout")

    def _create_result_from_job_output(self, job_output: Any) -> DAGValue:
        """Create DAGValue from job output."""
        # In a real implementation, you would parse the job output
        # and reconstruct the DAGValue
        import ray
        from feast.infra.compute_engines.dag.value import DAGValue, DAGFormat
        
        # Create mock result for now
        import pandas as pd
        mock_df = pd.DataFrame({
            "entity_id": [1, 2, 3],
            "feature_value": [10.5, 20.3, 30.1],
            "event_timestamp": [datetime.now()] * 3
        })
        
        ray_dataset = ray.data.from_pandas(mock_df)
        
        return DAGValue(
            data=ray_dataset,
            format=DAGFormat.RAY,
            metadata={
                "execution_time": datetime.now().isoformat(),
                "result_type": "codeflare_job_result",
            }
        )

    def _cleanup_lifecycle_cluster(self, cluster):
        """Cleanup lifecycle-managed cluster."""
        # Use CodeFlare SDK to cleanup cluster
        cluster.cleanup()
        logger.info("Lifecycle cluster cleaned up")

    def _cleanup_active_jobs(self):
        """Clean up active jobs."""
        logger.info("Cleaning up active jobs")
        
        for job_name in list(self.active_jobs.keys()):
            job_info = self.active_jobs[job_name]
            if "job" in job_info:
                try:
                    job_info["job"].cancel()
                except Exception as e:
                    logger.warning(f"Failed to cancel job {job_name}: {e}")
                    
            del self.active_jobs[job_name]
            
        logger.info("Active jobs cleaned up")

    def get_execution_status(self) -> Dict[str, Any]:
        """Get current execution status."""
        return {
            "active_jobs": len(self.active_jobs),
            "completed_steps": len(self.step_results),
            "cluster_mode": "existing" if self.use_existing_cluster else "lifecycle",
            "cluster_lifecycle_mode": self.cluster_lifecycle_mode,
            "job_details": {
                name: {
                    "status": info["status"],
                    "step_id": info["step_config"]["step_id"],
                    "start_time": info["start_time"].isoformat(),
                }
                for name, info in self.active_jobs.items()
            }
        }
