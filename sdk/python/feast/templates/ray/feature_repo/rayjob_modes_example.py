#!/usr/bin/env python3
"""
Comprehensive example demonstrating Feast RayJob CR execution modes.

This script shows the different ways to execute Feast Ray DAGs:
1. Custom RayJob CR orchestrator (creates new clusters per step)
2. CodeFlare SDK with existing cluster (reuses existing cluster)
3. CodeFlare SDK with lifecycle-managed clusters (per-job or per-step)
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


def demonstrate_execution_modes():
    """Demonstrate different RayJob CR execution modes."""
    
    print("🚀 Feast RayJob CR Execution Modes")
    print("=" * 60)
    
    # Mode 1: Custom RayJob CR Orchestrator (Default)
    print("\n📋 Mode 1: Custom RayJob CR Orchestrator")
    print("-" * 50)
    
    config1 = RayComputeEngineConfig(
        type="rayjob.engine",
        use_codeflare_sdk=False,  # Use custom orchestrator
        max_workers=5,
        namespace="default",
        step_timeout_seconds=1800,
    )
    
    print(f"   • Creates new RayJob CRs for each DAG step")
    print(f"   • Each RayJob CR creates its own RayCluster")
    print(f"   • Clusters are lifecycle-managed per step")
    print(f"   • No CodeFlare SDK dependency")
    print(f"   • Configuration: {config1.use_codeflare_sdk}")
    
    # Mode 2: CodeFlare SDK with Existing Cluster
    print("\n📋 Mode 2: CodeFlare SDK with Existing Cluster")
    print("-" * 50)
    
    config2 = RayComputeEngineConfig(
        type="rayjob.engine",
        use_codeflare_sdk=True,
        use_existing_cluster=True,
        existing_cluster_name="my-ray-cluster",
        cluster_lifecycle_mode="per_job",  # Not used with existing cluster
        max_workers=5,
        namespace="default",
        step_timeout_seconds=1800,
    )
    
    print(f"   • Submits jobs to existing Ray cluster")
    print(f"   • Cluster remains alive during entire DAG execution")
    print(f"   • Most efficient for repeated executions")
    print(f"   • Requires CodeFlare SDK and existing cluster")
    print(f"   • Configuration: {config2.use_existing_cluster}")
    
    # Mode 3: CodeFlare SDK with Lifecycle-Managed Cluster (Per Job)
    print("\n📋 Mode 3: CodeFlare SDK with Lifecycle Cluster (Per Job)")
    print("-" * 50)
    
    config3 = RayComputeEngineConfig(
        type="rayjob.engine",
        use_codeflare_sdk=True,
        use_existing_cluster=False,
        cluster_lifecycle_mode="per_job",
        max_workers=5,
        namespace="default",
        step_timeout_seconds=1800,
    )
    
    print(f"   • Creates one cluster for entire DAG execution")
    print(f"   • All DAG steps run on the same cluster")
    print(f"   • Cluster is cleaned up after DAG completion")
    print(f"   • Good balance of efficiency and isolation")
    print(f"   • Configuration: {config3.cluster_lifecycle_mode}")
    
    # Mode 4: CodeFlare SDK with Lifecycle-Managed Cluster (Per Step)
    print("\n📋 Mode 4: CodeFlare SDK with Lifecycle Cluster (Per Step)")
    print("-" * 50)
    
    config4 = RayComputeEngineConfig(
        type="rayjob.engine",
        use_codeflare_sdk=True,
        use_existing_cluster=False,
        cluster_lifecycle_mode="per_step",
        max_workers=5,
        namespace="default",
        step_timeout_seconds=1800,
    )
    
    print(f"   • Creates separate cluster for each DAG step")
    print(f"   • Maximum isolation between steps")
    print(f"   • Each cluster is cleaned up after step completion")
    print(f"   • Most resource-intensive but most fault-tolerant")
    print(f"   • Configuration: {config4.cluster_lifecycle_mode}")


def show_configuration_examples():
    """Show configuration examples for each mode."""
    
    print("\n🔧 Configuration Examples")
    print("=" * 60)
    
    print("\n1️⃣ Custom RayJob CR Orchestrator:")
    print("""
    batch_engine:
        type: rayjob.engine
        use_codeflare_sdk: false
        max_workers: 10
        namespace: default
        step_timeout_seconds: 3600
    """)
    
    print("\n2️⃣ CodeFlare SDK with Existing Cluster:")
    print("""
    batch_engine:
        type: rayjob.engine
        use_codeflare_sdk: true
        use_existing_cluster: true
        existing_cluster_name: my-ray-cluster
        max_workers: 10
        namespace: default
        step_timeout_seconds: 3600
    """)
    
    print("\n3️⃣ CodeFlare SDK with Lifecycle Cluster (Per Job):")
    print("""
    batch_engine:
        type: rayjob.engine
        use_codeflare_sdk: true
        use_existing_cluster: false
        cluster_lifecycle_mode: per_job
        max_workers: 10
        namespace: default
        step_timeout_seconds: 3600
    """)
    
    print("\n4️⃣ CodeFlare SDK with Lifecycle Cluster (Per Step):")
    print("""
    batch_engine:
        type: rayjob.engine
        use_codeflare_sdk: true
        use_existing_cluster: false
        cluster_lifecycle_mode: per_step
        max_workers: 10
        namespace: default
        step_timeout_seconds: 3600
    """)


def show_execution_flows():
    """Show execution flows for each mode."""
    
    print("\n🔄 Execution Flows")
    print("=" * 60)
    
    print("\n1️⃣ Custom RayJob CR Orchestrator:")
    print("   Step 1: Create RayJob CR → Create RayCluster → Execute RayReadNode → Destroy Cluster")
    print("   Step 2: Create RayJob CR → Create RayCluster → Execute RayJoinNode → Destroy Cluster")
    print("   Step 3: Create RayJob CR → Create RayCluster → Execute RayFilterNode → Destroy Cluster")
    print("   Step 4: Create RayJob CR → Create RayCluster → Execute RayAggregationNode → Destroy Cluster")
    
    print("\n2️⃣ CodeFlare SDK with Existing Cluster:")
    print("   Connect to existing cluster: my-ray-cluster")
    print("   Step 1: Submit job → Execute RayReadNode on existing cluster")
    print("   Step 2: Submit job → Execute RayJoinNode on existing cluster")
    print("   Step 3: Submit job → Execute RayFilterNode on existing cluster")
    print("   Step 4: Submit job → Execute RayAggregationNode on existing cluster")
    print("   Cluster remains alive for future use")
    
    print("\n3️⃣ CodeFlare SDK with Lifecycle Cluster (Per Job):")
    print("   Create cluster: feast-dag-cluster-123")
    print("   Step 1: Submit job → Execute RayReadNode on cluster")
    print("   Step 2: Submit job → Execute RayJoinNode on cluster")
    print("   Step 3: Submit job → Execute RayFilterNode on cluster")
    print("   Step 4: Submit job → Execute RayAggregationNode on cluster")
    print("   Destroy cluster: feast-dag-cluster-123")
    
    print("\n4️⃣ CodeFlare SDK with Lifecycle Cluster (Per Step):")
    print("   Step 1: Create cluster → Execute RayReadNode → Destroy cluster")
    print("   Step 2: Create cluster → Execute RayJoinNode → Destroy cluster")
    print("   Step 3: Create cluster → Execute RayFilterNode → Destroy cluster")
    print("   Step 4: Create cluster → Execute RayAggregationNode → Destroy cluster")


def show_benefits_and_tradeoffs():
    """Show benefits and tradeoffs of each mode."""
    
    print("\n⚖️ Benefits and Tradeoffs")
    print("=" * 60)
    
    print("\n1️⃣ Custom RayJob CR Orchestrator:")
    print("   ✅ Benefits:")
    print("      • No external dependencies")
    print("      • Maximum step isolation")
    print("      • Simple to understand")
    print("   ❌ Tradeoffs:")
    print("      • High resource overhead")
    print("      • Slow execution (cluster startup per step)")
    print("      • No cluster reuse")
    
    print("\n2️⃣ CodeFlare SDK with Existing Cluster:")
    print("   ✅ Benefits:")
    print("      • Fastest execution")
    print("      • Efficient resource usage")
    print("      • Cluster reuse across executions")
    print("   ❌ Tradeoffs:")
    print("      • Requires CodeFlare SDK")
    print("      • Requires existing cluster management")
    print("      • Less isolation between executions")
    
    print("\n3️⃣ CodeFlare SDK with Lifecycle Cluster (Per Job):")
    print("   ✅ Benefits:")
    print("      • Good balance of efficiency and isolation")
    print("      • Automatic cluster management")
    print("      • Step isolation within job")
    print("   ❌ Tradeoffs:")
    print("      • Requires CodeFlare SDK")
    print("      • Cluster startup overhead per job")
    
    print("\n4️⃣ CodeFlare SDK with Lifecycle Cluster (Per Step):")
    print("   ✅ Benefits:")
    print("      • Maximum fault tolerance")
    print("      • Complete step isolation")
    print("      • Automatic cluster management")
    print("   ❌ Tradeoffs:")
    print("      • Highest resource overhead")
    print("      • Slowest execution")
    print("      • Requires CodeFlare SDK")


def main():
    """Main demonstration function."""
    demonstrate_execution_modes()
    show_configuration_examples()
    show_execution_flows()
    show_benefits_and_tradeoffs()
    
    print("\n🎯 Recommendations")
    print("=" * 60)
    print("• Use Mode 2 (Existing Cluster) for production with frequent executions")
    print("• Use Mode 3 (Per Job) for balanced efficiency and isolation")
    print("• Use Mode 4 (Per Step) for maximum fault tolerance")
    print("• Use Mode 1 (Custom) only when CodeFlare SDK is not available")
    
    print("\n🔗 Dependencies")
    print("=" * 60)
    print("• Mode 1: KubeRay operator only")
    print("• Modes 2-4: KubeRay operator + CodeFlare SDK")
    print("• Install CodeFlare SDK: pip install codeflare-sdk")


if __name__ == "__main__":
    main()
