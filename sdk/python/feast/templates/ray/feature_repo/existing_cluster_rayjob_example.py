#!/usr/bin/env python3
"""
Example script demonstrating Feast RayJob CR execution on existing Ray cluster.

This script shows how to use CodeFlare SDK to submit RayJob CRs to an existing
Ray cluster, providing efficient cluster reuse with proper RayJob CR management.
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
    """Demonstrate Feast RayJob CR execution on existing Ray cluster."""
    
    print("🚀 Feast RayJob CR on Existing Ray Cluster")
    print("=" * 60)
    
    # Configuration for existing cluster + RayJob CR execution
    config = RayComputeEngineConfig(
        type="rayjob.engine",
        use_codeflare_sdk=True,           # Use CodeFlare SDK
        use_existing_cluster=True,        # Use existing Ray cluster
        existing_cluster_name="my-ray-cluster",  # Name of existing cluster
        max_workers=5,
        enable_optimization=True,
        namespace="default",
        step_timeout_seconds=1800,        # 30 minutes
        cluster_lifecycle_mode="per_job",
    )
    
    print(f"📋 Configuration:")
    print(f"   Engine Type: {config.type}")
    print(f"   CodeFlare SDK: {config.use_codeflare_sdk}")
    print(f"   Existing Cluster: {config.use_existing_cluster}")
    print(f"   Cluster Name: {config.existing_cluster_name}")
    print(f"   Max Workers: {config.max_workers}")
    print(f"   Namespace: {config.namespace}")
    print(f"   Step Timeout: {config.step_timeout_seconds}s")
    print(f"   Cluster Lifecycle: {config.cluster_lifecycle_mode}")
    print(f"   CodeFlare SDK handles: image, resources, monitoring automatically")
    print()
    
    # Example of how to use with FeatureStore
    print("🔧 Usage with FeatureStore:")
    print("""
    # In your feature_store.yaml:
    batch_engine:
        type: rayjob.engine
        use_codeflare_sdk: true
        use_existing_cluster: true
        existing_cluster_name: my-ray-cluster
        max_workers: 10
        enable_optimization: true
        namespace: default
        rayjob_image: rayproject/ray:2.8.0
        step_timeout_seconds: 3600
        enable_step_monitoring: true
        intermediate_storage_type: memory
    
    # In your Python code:
    from feast import FeatureStore
    
    store = FeatureStore(repo_path=".")
    
    # Get historical features - will submit RayJob CRs to existing cluster
    features = store.get_historical_features(
        entity_df=entity_df,
        features=["feature_view:feature1", "feature_view:feature2"]
    ).to_df()
    
    # Materialize features - will submit RayJob CRs to existing cluster
    store.materialize(
        feature_views=["feature_view"],
        start_date=datetime.now() - timedelta(days=1),
        end_date=datetime.now()
    )
    """)
    
    print("✅ Example completed!")
    print()
    print("📚 Key Features:")
    print("   • RayJob CRs submitted to existing Ray cluster")
    print("   • Cluster remains alive during entire DAG execution")
    print("   • Efficient resource usage and cluster reuse")
    print("   • Proper RayJob CR management via CodeFlare SDK")
    print("   • Step isolation within RayJob CRs")
    print("   • Automatic step dependency management")
    print()
    print("🔄 Execution Flow:")
    print("   1. Connect to existing Ray cluster: my-ray-cluster")
    print("   2. Submit RayJob CR 'feast-dag-step-read-001' to existing cluster")
    print("   3. Submit RayJob CR 'feast-dag-step-join-002' to existing cluster (waits for step 1)")
    print("   4. Submit RayJob CR 'feast-dag-step-filter-003' to existing cluster (waits for step 2)")
    print("   5. Submit RayJob CR 'feast-dag-step-aggregate-004' to existing cluster (waits for step 3)")
    print("   6. Collect final result from step 4")
    print("   7. Cluster remains alive for future use")
    print()
    print("🎯 Benefits:")
    print("   • Fastest execution (no cluster startup overhead)")
    print("   • Efficient resource usage (cluster reuse)")
    print("   • Proper RayJob CR management and monitoring")
    print("   • Step isolation within RayJob CRs")
    print("   • Automatic dependency resolution")
    print("   • Cluster remains available for other workloads")
    print()
    print("🔗 Dependencies:")
    print("   • Install CodeFlare SDK: pip install codeflare-sdk")
    print("   • Install KubeRay operator in your Kubernetes cluster")
    print("   • Create and maintain existing Ray cluster")
    print("   • Configure kubectl to access your Kubernetes cluster")
    print()
    print("📊 Monitoring:")
    print("   • Check RayJob CR status: kubectl get rayjobs")
    print("   • View RayJob CR logs: kubectl logs <rayjob-pod-name>")
    print("   • Monitor existing cluster: kubectl get rayclusters")
    print("   • Check step progress via Feast execution status")
    print()
    print("🏗️ Prerequisites:")
    print("   1. Existing Ray cluster running in Kubernetes")
    print("   2. KubeRay operator installed")
    print("   3. CodeFlare SDK installed")
    print("   4. Proper cluster labeling for RayJob CR targeting")


def show_rayjob_cr_manifest():
    """Show example RayJob CR manifest for existing cluster."""
    
    print("\n📋 Example RayJob CR Manifest:")
    print("=" * 60)
    
    manifest = """
apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: feast-dag-step-read-001
  namespace: default
  labels:
    feast-step: read
    feast-dag-execution: "true"
    feast-cluster-mode: existing
spec:
  entrypoint: python -c "import os; import json; from feast.infra.compute_engines.ray.dag_step_executor import RayJobDAGStepRunner; runner = RayJobDAGStepRunner(); runner.run_step(os.environ.get('FEAST_STEP_CONFIG', '{}'), None)"
  runtimeEnv:
    pip:
      - feast
      - ray[data]
      - pandas
      - pyarrow
      - dill
    env_vars:
      FEAST_STEP_ID: read
      FEAST_STEP_CONFIG: '{"step_id": "read", "node_type": "RayReadNode", ...}'
  clusterSelector:
    matchLabels:
      ray.io/cluster-name: my-ray-cluster
  shutdownAfterJobFinishes: false
  ttlSecondsAfterFinished: 300
"""
    
    print(manifest)


def show_cluster_setup():
    """Show how to set up existing Ray cluster."""
    
    print("\n🏗️ Setting up Existing Ray Cluster:")
    print("=" * 60)
    
    print("""
# 1. Create RayCluster CR
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: my-ray-cluster
  namespace: default
spec:
  rayVersion: "2.8.0"
  headGroupSpec:
    rayStartParams: {}
    template:
      spec:
        containers:
        - name: ray-head
          image: rayproject/ray:2.8.0
          resources:
            requests:
              cpu: 1
              memory: 2Gi
  workerGroupSpecs:
  - replicas: 3
    minReplicas: 1
    maxReplicas: 5
    template:
      spec:
        containers:
        - name: ray-worker
          image: rayproject/ray:2.8.0
          resources:
            requests:
              cpu: 1
              memory: 2Gi

# 2. Apply the cluster
kubectl apply -f raycluster.yaml

# 3. Verify cluster is running
kubectl get rayclusters
kubectl get pods -l ray.io/cluster-name=my-ray-cluster
""")


if __name__ == "__main__":
    main()
    show_rayjob_cr_manifest()
    show_cluster_setup()
