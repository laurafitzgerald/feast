"""
DAG Serialization for RayJob CR Execution

This module provides serialization capabilities for Feast Ray DAG nodes,
enabling them to be submitted as discrete RayJob CRs on Kubernetes.
"""

import json
import logging
import pickle
from typing import Any, Dict, List, Optional, Union

import dill
import ray

from feast.infra.compute_engines.dag.context import ExecutionContext
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.dag.value import DAGValue
from feast.infra.compute_engines.ray.config import RayComputeEngineConfig
from feast.infra.compute_engines.ray.nodes import (
    RayAggregationNode,
    RayDedupNode,
    RayFilterNode,
    RayJoinNode,
    RayReadNode,
    RayTransformationNode,
    RayWriteNode,
)

logger = logging.getLogger(__name__)


class DAGStepSerializer:
    """
    Serializes DAG nodes and context for RayJob CR submission.
    
    This class handles the conversion of Feast DAG nodes into serializable
    configurations that can be submitted as discrete RayJob CRs.
    """

    def __init__(self, config: RayComputeEngineConfig):
        self.config = config

    def serialize_execution_plan(self, execution_plan, context: ExecutionContext) -> Dict[str, Any]:
        """
        Serialize an entire execution plan into discrete steps.
        
        Args:
            execution_plan: The ExecutionPlan to serialize
            context: The ExecutionContext containing runtime information
            
        Returns:
            Dictionary containing serialized steps and metadata
        """
        steps = []
        step_dependencies = {}
        
        # Serialize each node as a discrete step
        for i, node in enumerate(execution_plan.nodes):
            step_config = self.serialize_dag_step(node, context, i)
            steps.append(step_config)
            
            # Track dependencies
            step_dependencies[node.name] = [
                inp.name for inp in node.inputs
            ]
        
        return {
            "steps": steps,
            "dependencies": step_dependencies,
            "total_steps": len(steps),
            "execution_order": [node.name for node in execution_plan.nodes],
            "metadata": {
                "serializer_version": "1.0",
                "config": self.config.dict() if hasattr(self.config, 'dict') else {},
            }
        }

    def serialize_dag_step(
        self, 
        node: DAGNode, 
        context: ExecutionContext, 
        step_index: int
    ) -> Dict[str, Any]:
        """
        Serialize a single DAG node into a step configuration.
        
        Args:
            node: The DAGNode to serialize
            context: The ExecutionContext
            step_index: Index of this step in the execution plan
            
        Returns:
            Dictionary containing step configuration
        """
        step_config = {
            "step_id": node.name,
            "step_index": step_index,
            "node_type": type(node).__name__,
            "input_dependencies": [inp.name for inp in node.inputs],
            "step_metadata": {
                "execution_order": step_index,
                "resource_requirements": self._estimate_resources(node),
                "estimated_duration": self._estimate_duration(node),
            }
        }
        
        # Serialize node-specific configuration
        step_config["node_config"] = self._serialize_node_config(node)
        
        # Serialize context snapshot (excluding large data)
        step_config["context_snapshot"] = self._serialize_context_snapshot(context)
        
        return step_config

    def _serialize_node_config(self, node: DAGNode) -> Dict[str, Any]:
        """Serialize node-specific configuration."""
        config = {
            "node_name": node.name,
            "node_type": type(node).__name__,
        }
        
        # Handle different node types
        if isinstance(node, RayReadNode):
            config.update({
                "source_type": type(node.source).__name__,
                "start_time": node.start_time.isoformat() if node.start_time else None,
                "end_time": node.end_time.isoformat() if node.end_time else None,
                "column_info": self._serialize_column_info(node.column_info),
            })
            
        elif isinstance(node, RayJoinNode):
            config.update({
                "join_type": getattr(node, "join_type", "left"),
                "entity_keys": getattr(node, "entity_keys", []),
                "feature_keys": getattr(node, "feature_keys", []),
            })
            
        elif isinstance(node, RayFilterNode):
            config.update({
                "filter_conditions": getattr(node, "filter_conditions", []),
                "ttl_seconds": getattr(node, "ttl_seconds", None),
            })
            
        elif isinstance(node, RayAggregationNode):
            config.update({
                "aggregations": [
                    {
                        "function": agg.function,
                        "column": agg.column,
                        "time_window": agg.time_window.total_seconds() if agg.time_window else None,
                    }
                    for agg in node.aggregations
                ],
                "group_by_keys": node.group_by_keys,
                "timestamp_col": node.timestamp_col,
            })
            
        elif isinstance(node, RayTransformationNode):
            # Serialize transformation function
            transformation_serialized = None
            if hasattr(node.transformation, "udf") and callable(node.transformation.udf):
                transformation_serialized = dill.dumps(node.transformation.udf)
            elif callable(node.transformation):
                transformation_serialized = dill.dumps(node.transformation)
                
            config.update({
                "transformation_name": getattr(node.transformation, "name", "unknown"),
                "transformation_serialized": transformation_serialized,
            })
            
        elif isinstance(node, RayDedupNode):
            config.update({
                "dedup_keys": getattr(node, "dedup_keys", []),
                "timestamp_col": getattr(node, "timestamp_col", None),
            })
            
        elif isinstance(node, RayWriteNode):
            config.update({
                "write_target": getattr(node, "write_target", None),
                "write_format": getattr(node, "write_format", "parquet"),
            })
        
        return config

    def _serialize_context_snapshot(self, context: ExecutionContext) -> Dict[str, Any]:
        """Serialize context snapshot, excluding large data objects."""
        snapshot = {
            "project": context.project,
            "feature_view_name": context.feature_view_name,
            "entity_df_shape": getattr(context.entity_df, 'shape', None) if hasattr(context, 'entity_df') else None,
            "entity_df_columns": list(context.entity_df.columns) if hasattr(context, 'entity_df') and hasattr(context.entity_df, 'columns') else [],
            "registry_path": getattr(context, 'registry_path', None),
            "offline_store_config": getattr(context, 'offline_store_config', {}),
            "online_store_config": getattr(context, 'online_store_config', {}),
        }
        
        # Add any additional context metadata
        if hasattr(context, 'additional_metadata'):
            snapshot["additional_metadata"] = context.additional_metadata
            
        return snapshot

    def _serialize_column_info(self, column_info) -> Dict[str, Any]:
        """Serialize column information."""
        if column_info is None:
            return {}
            
        return {
            "columns": getattr(column_info, 'columns', []),
            "timestamp_column": getattr(column_info, 'timestamp_column', None),
            "created_timestamp_column": getattr(column_info, 'created_timestamp_column', None),
        }

    def _estimate_resources(self, node: DAGNode) -> Dict[str, Any]:
        """Estimate resource requirements for a DAG node."""
        base_resources = {
            "cpu": 1,
            "memory": "1Gi",
            "gpu": 0,
        }
        
        # Adjust based on node type
        if isinstance(node, RayReadNode):
            base_resources["cpu"] = 2
            base_resources["memory"] = "2Gi"
        elif isinstance(node, RayAggregationNode):
            base_resources["cpu"] = 2
            base_resources["memory"] = "4Gi"
        elif isinstance(node, RayTransformationNode):
            base_resources["cpu"] = 1
            base_resources["memory"] = "2Gi"
            
        return base_resources

    def _estimate_duration(self, node: DAGNode) -> int:
        """Estimate execution duration in seconds."""
        # Base duration estimates
        duration_map = {
            RayReadNode: 30,
            RayJoinNode: 60,
            RayFilterNode: 10,
            RayAggregationNode: 120,
            RayTransformationNode: 45,
            RayDedupNode: 20,
            RayWriteNode: 30,
        }
        
        return duration_map.get(type(node), 60)


class DAGStepDeserializer:
    """
    Deserializes DAG step configurations back into executable nodes.
    
    This class handles the reconstruction of DAG nodes from serialized
    configurations within RayJob CR execution environments.
    """

    def __init__(self, config: RayComputeEngineConfig):
        self.config = config

    def deserialize_step(self, step_config: Dict[str, Any]) -> DAGNode:
        """
        Deserialize a step configuration back into a DAG node.
        
        Args:
            step_config: The serialized step configuration
            
        Returns:
            Reconstructed DAGNode
        """
        node_type = step_config["node_type"]
        node_config = step_config["node_config"]
        
        if node_type == "RayReadNode":
            return self._deserialize_read_node(node_config)
        elif node_type == "RayJoinNode":
            return self._deserialize_join_node(node_config)
        elif node_type == "RayFilterNode":
            return self._deserialize_filter_node(node_config)
        elif node_type == "RayAggregationNode":
            return self._deserialize_aggregation_node(node_config)
        elif node_type == "RayTransformationNode":
            return self._deserialize_transformation_node(node_config)
        elif node_type == "RayDedupNode":
            return self._deserialize_dedup_node(node_config)
        elif node_type == "RayWriteNode":
            return self._deserialize_write_node(node_config)
        else:
            raise ValueError(f"Unknown node type: {node_type}")

    def _deserialize_read_node(self, config: Dict[str, Any]) -> RayReadNode:
        """Deserialize RayReadNode."""
        from datetime import datetime
        
        start_time = None
        if config.get("start_time"):
            start_time = datetime.fromisoformat(config["start_time"])
            
        end_time = None
        if config.get("end_time"):
            end_time = datetime.fromisoformat(config["end_time"])
        
        # Note: In a real implementation, you'd need to reconstruct the DataSource
        # This is a simplified version for demonstration
        return RayReadNode(
            name=config["node_name"],
            source=None,  # Would need to be reconstructed from config
            column_info=None,  # Would need to be reconstructed from config
            config=self.config,
            start_time=start_time,
            end_time=end_time,
        )

    def _deserialize_join_node(self, config: Dict[str, Any]) -> RayJoinNode:
        """Deserialize RayJoinNode."""
        return RayJoinNode(
            name=config["node_name"],
            entity_keys=config.get("entity_keys", []),
            feature_keys=config.get("feature_keys", []),
            config=self.config,
        )

    def _deserialize_filter_node(self, config: Dict[str, Any]) -> RayFilterNode:
        """Deserialize RayFilterNode."""
        return RayFilterNode(
            name=config["node_name"],
            filter_conditions=config.get("filter_conditions", []),
            config=self.config,
        )

    def _deserialize_aggregation_node(self, config: Dict[str, Any]) -> RayAggregationNode:
        """Deserialize RayAggregationNode."""
        from feast.aggregation import Aggregation
        from datetime import timedelta
        
        aggregations = []
        for agg_config in config.get("aggregations", []):
            time_window = None
            if agg_config.get("time_window"):
                time_window = timedelta(seconds=agg_config["time_window"])
                
            aggregations.append(Aggregation(
                function=agg_config["function"],
                column=agg_config["column"],
                time_window=time_window,
            ))
        
        return RayAggregationNode(
            name=config["node_name"],
            aggregations=aggregations,
            group_by_keys=config.get("group_by_keys", []),
            timestamp_col=config.get("timestamp_col", "event_timestamp"),
            config=self.config,
        )

    def _deserialize_transformation_node(self, config: Dict[str, Any]) -> RayTransformationNode:
        """Deserialize RayTransformationNode."""
        transformation = None
        if config.get("transformation_serialized"):
            transformation = dill.loads(config["transformation_serialized"])
        
        return RayTransformationNode(
            name=config["node_name"],
            transformation=transformation,
            config=self.config,
        )

    def _deserialize_dedup_node(self, config: Dict[str, Any]) -> RayDedupNode:
        """Deserialize RayDedupNode."""
        return RayDedupNode(
            name=config["node_name"],
            dedup_keys=config.get("dedup_keys", []),
            config=self.config,
        )

    def _deserialize_write_node(self, config: Dict[str, Any]) -> RayWriteNode:
        """Deserialize RayWriteNode."""
        return RayWriteNode(
            name=config["node_name"],
            write_target=config.get("write_target"),
            config=self.config,
        )
