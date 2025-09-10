#!/usr/bin/env python3
"""
Example script demonstrating Feast RayJob CR-based DAG execution.

This script shows how to use the new rayjob.engine batch engine type
to execute Feast Ray DAGs as discrete RayJob CRs on Kubernetes.
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add the Feast SDK to the Python path
feast_sdk_path = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(feast_sdk_path))

from feast import FeatureStore
from feast.infra.compute_engines.ray.config import RayComputeEngineConfig


def main():
    """Demonstrate Feast RayJob CR-based DAG execution."""
    
    print("🚀 Feast RayJob CR DAG Execution Example")
    print("=" * 60)
    
    # Example configuration for RayJob CR execution
    config = RayComputeEngineConfig(
        type="rayjob.engine",
        max_workers=5,
        enable_optimization=True,
        namespace="default",
        rayjob_image="rayproject/ray:2.8.0",
        step_timeout_seconds=1800,  # 30 minutes
        enable_step_monitoring=True,
        intermediate_storage_type="memory",
    )
    
    print(f"📋 RayJob CR Configuration:")
    print(f"   Engine Type: {config.type}")
    print(f"   Max Workers: {config.max_workers}")
    print(f"   Namespace: {config.namespace}")
    print(f"   Ray Image: {config.rayjob_image}")
    print(f"   Step Timeout: {config.step_timeout_seconds}s")
    print(f"   Step Monitoring: {config.enable_step_monitoring}")
    print(f"   Intermediate Storage: {config.intermediate_storage_type}")
    print()
    
    # Example of how to use with FeatureStore
    print("🔧 Usage with FeatureStore:")
    print("""
    # In your feature_store.yaml:
    batch_engine:
        type: rayjob.engine
        max_workers: 10
        enable_optimization: true
        namespace: default
        rayjob_image: rayproject/ray:2.8.0
        step_timeout_seconds: 3600
        enable_step_monitoring: true
        intermediate_storage_type: memory
        intermediate_storage_config:
            redis_host: localhost
            redis_port: 6379
    
    # In your Python code:
    from feast import FeatureStore
    
    store = FeatureStore(repo_path=".")
    
    # Get historical features - will execute via discrete RayJob CRs
    features = store.get_historical_features(
        entity_df=entity_df,
        features=["feature_view:feature1", "feature_view:feature2"]
    ).to_df()
    
    # Materialize features - will execute via discrete RayJob CRs
    store.materialize(
        feature_views=["feature_view"],
        start_date=datetime.now() - timedelta(days=1),
        end_date=datetime.now()
    )
    """)
    
    print("✅ Example completed!")
    print()
    print("📚 Key Features of RayJob CR DAG Execution:")
    print("   • Discrete Step Execution: Each DAG node runs in its own RayJob CR")
    print("   • Step Isolation: Independent resource allocation per step")
    print("   • Fault Tolerance: Failed steps can be retried without affecting others")
    print("   • Independent Scaling: Each step scales based on its workload")
    print("   • Better Monitoring: Each step has its own RayJob status and logs")
    print("   • Resource Optimization: Different steps can use different resource profiles")
    print()
    print("🔄 DAG Execution Flow:")
    print("   1. RayReadNode → RayJob CR 'feast-dag-step-read-001'")
    print("   2. RayJoinNode → RayJob CR 'feast-dag-step-join-002' (waits for step 1)")
    print("   3. RayFilterNode → RayJob CR 'feast-dag-step-filter-003' (waits for step 2)")
    print("   4. RayAggregationNode → RayJob CR 'feast-dag-step-aggregate-004' (waits for step 3)")
    print("   5. Collect final result from step 4")
    print()
    print("🔗 Dependencies:")
    print("   • Install KubeRay operator in your Kubernetes cluster")
    print("   • Configure kubectl to access your Kubernetes cluster")
    print("   • Ensure RayJob CRs are supported in your cluster")
    print()
    print("📊 Monitoring:")
    print("   • Check RayJob CR status: kubectl get rayjobs")
    print("   • View step logs: kubectl logs <rayjob-pod-name>")
    print("   • Monitor step progress via Feast execution status")


if __name__ == "__main__":
    main()
