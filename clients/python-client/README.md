# KubeRay Python Client

This Python client library provides APIs to handle `RayCluster` and `RayJob` resources from your Python application.

## Prerequisites

It is assumed that your Kubernetes cluster is already set up. Your kubectl configuration is expected to be in `~/.kube/config` if you are running the code directly from your terminal.

It is also expected that the `KubeRay operator` is installed.
[Installation instructions are here][quick-start]

## Installation

### From PyPI (Recommended)

```bash
pip install kuberay-client
```

### From Source

```bash
git clone https://github.com/ray-project/kuberay.git
cd kuberay/clients/python-client
pip install -e .
```

## Usage

There are multiple levels of using the API with increasing levels of complexity.

### Director

This is the easiest form of using the API to create Ray clusters with predefined cluster sizes:

```python
from python_client import kuberay_cluster_api
from python_client.utils import kuberay_cluster_builder

# Initialize the APIs
cluster_api = kuberay_cluster_api.RayClusterApi()
director = kuberay_cluster_builder.Director()

# Create a small cluster
cluster_config = director.build_small_cluster(name="my-cluster")
if cluster_config:
    cluster_api.create_ray_cluster(body=cluster_config)
```

### Cluster Builder

The builder allows you to build the cluster piece by piece with more granular control:

```python
from python_client import kuberay_cluster_api
from python_client.utils import kuberay_cluster_builder

cluster_api = kuberay_cluster_api.RayClusterApi()
builder = kuberay_cluster_builder.ClusterBuilder()

cluster = (
    builder.build_meta(name="custom-cluster")
    .build_head()
    .build_worker(group_name="workers", replicas=3)
    .get_cluster()
)

if builder.succeeded:
    cluster_api.create_ray_cluster(body=cluster)
```

### RayJob Management

Submit and manage Ray jobs:

```python
from python_client import kuberay_job_api, kuberay_cluster_api
from python_client import constants

# Initialize APIs
job_api = kuberay_job_api.RayjobApi()
cluster_api = kuberay_cluster_api.RayClusterApi()

# Create a job specification
job_spec = {
    "apiVersion": f"{constants.GROUP}/{constants.JOB_VERSION}",
    "kind": constants.JOB_KIND,
    "metadata": {
        "name": "my-ray-job",
        "namespace": "default"
    },
    "spec": {
        "rayCluster": "my-cluster",
        "entrypoint": "python my_script.py",
        "submissionMode": "K8sJobMode"
    }
}

# Submit the job
job = job_api.submit_job(job=job_spec, k8s_namespace="default")

# Wait for completion
success = job_api.wait_until_job_finished("my-ray-job", "default", timeout=300)

# Get job status
status = job_api.get_job_status("my-ray-job", "default")

# Clean up
job_api.delete_job("my-ray-job", "default")
```

### Cluster Utils

`cluster_utils` gives you even more options to modify your cluster definition:

```python
from python_client.utils import kuberay_cluster_utils

cluster_utils = kuberay_cluster_utils.ClusterUtils()

# Update worker group replicas
updated_cluster, success = cluster_utils.update_worker_group_replicas(
    cluster, group_name="workers", max_replicas=4, min_replicas=1, replicas=2
)

if success:
    cluster_api.patch_ray_cluster(
        name=updated_cluster["metadata"]["name"], 
        ray_patch=updated_cluster
    )
```

### Raw API Usage

You can also use raw JSON configurations if you prefer:

```python
cluster_config = {
    "apiVersion": "ray.io/v1",
    "kind": "RayCluster",
    "metadata": {"name": "raw-cluster"},
    "spec": {
        # ... your cluster specification
    }
}

cluster_api.create_ray_cluster(body=cluster_config)
```

## API Reference

### RayClusterApi

- `list_ray_clusters(k8s_namespace="default")` - List all Ray clusters
- `get_ray_cluster(name, k8s_namespace="default")` - Get a specific cluster
- `create_ray_cluster(body, k8s_namespace="default")` - Create a new cluster
- `delete_ray_cluster(name, k8s_namespace="default")` - Delete a cluster
- `patch_ray_cluster(name, ray_patch, k8s_namespace="default")` - Update a cluster
- `get_ray_cluster_status(name, k8s_namespace="default")` - Get cluster status
- `wait_until_ray_cluster_running(name, k8s_namespace="default")` - Wait for cluster to be ready

### RayjobApi

- `submit_job(job, k8s_namespace="default")` - Submit a Ray job
- `get_job_status(name, k8s_namespace="default")` - Get job status
- `wait_until_job_finished(name, k8s_namespace="default")` - Wait for job completion
- `delete_job(name, k8s_namespace="default")` - Delete a job

## Development

### Setup

```bash
git clone https://github.com/ray-project/kuberay.git
cd kuberay/clients/python-client
pip install -e .
```

### Running Tests

```bash
# Install test dependencies
pip install pytest

# Run tests
pytest python_client_test/
```

### Code Organization

```text
clients/python-client/
├── README.md
├── pyproject.toml
├── python_client/
│   ├── __init__.py
│   ├── constants.py
│   ├── kuberay_cluster_api.py
│   ├── kuberay_job_api.py
│   └── utils/
│       ├── __init__.py
│       ├── kuberay_cluster_builder.py
│       └── kuberay_cluster_utils.py
├── python_client_test/
│   ├── test_cluster_api.py
│   ├── test_job_api.py
│   ├── test_director.py
│   └── test_utils.py
└── examples/
    ├── cluster_management.py
    ├── job_management.py
    └── complete_example.py
```

## Contributing

We welcome contributions! Please see the [KubeRay contributing guide](https://github.com/ray-project/kuberay/blob/master/CONTRIBUTING.md) for details.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

[quick-start]: https://github.com/ray-project/kuberay#quick-start
