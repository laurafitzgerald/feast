"""
DAG Step Executor for RayJob CR Execution

This module provides the execution engine for individual DAG steps
within RayJob CRs, handling step isolation and result storage.
"""

import json
import logging
import os
import pickle
from typing import Any, Dict, List, Optional, Union

import ray
from ray.data import Dataset

from feast.infra.compute_engines.dag.context import ExecutionContext
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.dag.value import DAGValue, DAGFormat
from feast.infra.compute_engines.ray.config import RayComputeEngineConfig
from feast.infra.compute_engines.ray.dag_serializer import DAGStepDeserializer

logger = logging.getLogger(__name__)


class DAGStepExecutor:
    """
    Executes individual DAG steps within RayJob CRs.
    
    This class handles the execution of single DAG nodes in isolated
    RayJob CR environments, including input loading and result storage.
    """

    def __init__(self, config: RayComputeEngineConfig):
        self.config = config
        self.deserializer = DAGStepDeserializer(config)
        self.step_results = {}

    def execute_step(
        self, 
        step_config: Dict[str, Any], 
        input_data: Optional[Dict[str, Any]] = None
    ) -> DAGValue:
        """
        Execute a single DAG step.
        
        Args:
            step_config: Serialized step configuration
            input_data: Input data from previous steps
            
        Returns:
            DAGValue containing step results
        """
        step_id = step_config["step_id"]
        logger.info(f"Executing DAG step: {step_id}")
        
        try:
            # Deserialize the DAG node
            node = self.deserializer.deserialize_step(step_config)
            
            # Load input data from previous step(s)
            input_values = self._load_input_data(input_data, step_config)
            
            # Create execution context for this step
            context = self._create_step_context(step_config, input_values)
            
            # Execute the node
            result = node.execute(context)
            
            # Store result for next step
            self._store_step_result(step_id, result)
            
            logger.info(f"Successfully executed DAG step: {step_id}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to execute DAG step {step_id}: {e}")
            raise

    def _load_input_data(
        self, 
        input_data: Optional[Dict[str, Any]], 
        step_config: Dict[str, Any]
    ) -> Dict[str, DAGValue]:
        """
        Load input data from previous steps.
        
        Args:
            input_data: Raw input data dictionary
            step_config: Current step configuration
            
        Returns:
            Dictionary of input DAGValues
        """
        input_values = {}
        dependencies = step_config.get("input_dependencies", [])
        
        if not input_data:
            return input_values
            
        for dep_name in dependencies:
            if dep_name in input_data:
                # Convert input data to DAGValue
                dep_data = input_data[dep_name]
                
                if isinstance(dep_data, Dataset):
                    dag_value = DAGValue(data=dep_data, format=DAGFormat.RAY)
                elif isinstance(dep_data, dict) and "data" in dep_data:
                    # Handle serialized DAGValue
                    dag_value = self._deserialize_dag_value(dep_data)
                else:
                    # Convert to Ray Dataset
                    if hasattr(dep_data, 'to_pandas'):
                        df = dep_data.to_pandas()
                    elif hasattr(dep_data, 'to_arrow'):
                        df = dep_data.to_arrow().to_pandas()
                    else:
                        df = dep_data
                    
                    ray_dataset = ray.data.from_pandas(df)
                    dag_value = DAGValue(data=ray_dataset, format=DAGFormat.RAY)
                
                input_values[dep_name] = dag_value
                
        return input_values

    def _create_step_context(
        self, 
        step_config: Dict[str, Any], 
        input_values: Dict[str, DAGValue]
    ) -> ExecutionContext:
        """
        Create execution context for a single step.
        
        Args:
            step_config: Step configuration
            input_values: Input DAGValues from previous steps
            
        Returns:
            ExecutionContext for step execution
        """
        context_snapshot = step_config.get("context_snapshot", {})
        
        # Create a minimal execution context
        context = ExecutionContext(
            project=context_snapshot.get("project", "default"),
            feature_view_name=context_snapshot.get("feature_view_name", "unknown"),
            entity_df=None,  # Will be loaded from input if needed
        )
        
        # Add input values as node outputs for dependency resolution
        context.node_outputs = input_values
        
        # Add additional context metadata
        if "additional_metadata" in context_snapshot:
            context.additional_metadata = context_snapshot["additional_metadata"]
            
        return context

    def _store_step_result(self, step_id: str, result: DAGValue):
        """
        Store step result for next steps.
        
        Args:
            step_id: Step identifier
            result: DAGValue result
        """
        # Store in memory for this executor instance
        self.step_results[step_id] = result
        
        # In a real implementation, you might also store to:
        # - Redis for cross-step communication
        # - S3 for large datasets
        # - Ray object store for Ray cluster communication
        
        logger.info(f"Stored result for step {step_id}")

    def _deserialize_dag_value(self, serialized_value: Dict[str, Any]) -> DAGValue:
        """
        Deserialize a DAGValue from serialized format.
        
        Args:
            serialized_value: Serialized DAGValue dictionary
            
        Returns:
            Deserialized DAGValue
        """
        data = serialized_value.get("data")
        format_str = serialized_value.get("format", "RAY")
        metadata = serialized_value.get("metadata", {})
        
        # Convert format string to DAGFormat enum
        if format_str == "RAY":
            format_enum = DAGFormat.RAY
        else:
            format_enum = DAGFormat.RAY  # Default fallback
            
        # Reconstruct Ray Dataset if needed
        if isinstance(data, dict) and "ray_dataset_id" in data:
            # In a real implementation, you'd reconstruct the Ray Dataset
            # from the stored reference
            ray_dataset = ray.data.from_pandas(data.get("fallback_data", []))
        else:
            ray_dataset = data
            
        return DAGValue(
            data=ray_dataset,
            format=format_enum,
            metadata=metadata
        )

    def get_step_result(self, step_id: str) -> Optional[DAGValue]:
        """
        Get stored step result.
        
        Args:
            step_id: Step identifier
            
        Returns:
            Stored DAGValue or None
        """
        return self.step_results.get(step_id)

    def cleanup_step_results(self):
        """Clean up stored step results."""
        self.step_results.clear()
        logger.info("Cleaned up step results")


class RayJobDAGStepRunner:
    """
    Main entry point for DAG step execution within RayJob CRs.
    
    This class serves as the entry point script that runs within
    RayJob CR containers to execute individual DAG steps.
    """

    def __init__(self):
        self.config = None
        self.executor = None

    def run_step(self, step_config_json: str, input_data_json: Optional[str] = None):
        """
        Main entry point for step execution.
        
        Args:
            step_config_json: JSON string of step configuration
            input_data_json: JSON string of input data (optional)
        """
        try:
            # Parse configuration
            step_config = json.loads(step_config_json)
            input_data = json.loads(input_data_json) if input_data_json else None
            
            # Create config from step metadata
            config_dict = step_config.get("step_metadata", {}).get("config", {})
            self.config = RayComputeEngineConfig(**config_dict)
            
            # Create executor
            self.executor = DAGStepExecutor(self.config)
            
            # Execute step
            result = self.executor.execute_step(step_config, input_data)
            
            # Serialize result for output
            result_data = self._serialize_result(result)
            
            # Output result (in a real implementation, this would be stored)
            print(f"STEP_RESULT: {json.dumps(result_data)}")
            
        except Exception as e:
            logger.error(f"Step execution failed: {e}")
            print(f"STEP_ERROR: {str(e)}")
            raise

    def _serialize_result(self, result: DAGValue) -> Dict[str, Any]:
        """
        Serialize DAGValue result for output.
        
        Args:
            result: DAGValue to serialize
            
        Returns:
            Serialized result dictionary
        """
        # Convert Ray Dataset to serializable format
        if isinstance(result.data, Dataset):
            # In a real implementation, you'd store the dataset reference
            # and return metadata about the result
            return {
                "format": result.format.value,
                "metadata": result.metadata,
                "data_shape": "ray_dataset",  # Placeholder
                "data_type": "Dataset",
            }
        else:
            return {
                "format": result.format.value,
                "metadata": result.metadata,
                "data": str(result.data),
                "data_type": type(result.data).__name__,
            }


def main():
    """
    Main entry point for RayJob CR execution.
    
    This function is called by the RayJob CR entrypoint script.
    """
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python dag_step_runner.py <step_config_json> [input_data_json]")
        sys.exit(1)
        
    step_config_json = sys.argv[1]
    input_data_json = sys.argv[2] if len(sys.argv) > 2 else None
    
    runner = RayJobDAGStepRunner()
    runner.run_step(step_config_json, input_data_json)


if __name__ == "__main__":
    main()
