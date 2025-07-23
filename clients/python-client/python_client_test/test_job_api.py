import unittest
import uuid

from python_client import kuberay_job_api
from python_client import kuberay_cluster_api
from python_client import constants
from python_client.utils import kuberay_cluster_builder


class TestJobApi(unittest.TestCase):
    def setUp(self) -> None:
        self.job_api = kuberay_job_api.RayjobApi()
        self.cluster_api = kuberay_cluster_api.RayClusterApi()
        self.director = kuberay_cluster_builder.Director()

    def test_wait_until_job_finished(self):
        """Test wait_until_job_finished method."""
        # Test case 1: job not found
        job_name = "nonexistent-job"
        namespace = "default"
        timeout = 10
        interval = 1
        result = self.job_api.wait_until_job_finished(
            job_name, namespace, timeout, interval
        )
        self.assertFalse(result, "Should return False for non-existent job")

    def test_submit_ray_job_to_existing_cluster(self):
        """Test submit_ray_job_to_existing_cluster method."""
        # Create a RayCluster first - use shorter names
        cluster_name = "test-" + str(uuid.uuid4())[:8]
        namespace = "default"
        cluster_body = self.director.build_small_cluster(
            name=cluster_name,
            k8s_namespace=namespace,
            labels={"ray.io/cluster": cluster_name}
        )
        created_cluster = self.cluster_api.create_ray_cluster(
            body=cluster_body, k8s_namespace=namespace
        )
        self.cluster_api.wait_until_ray_cluster_running(cluster_name, namespace, 120, 10)

        # Create a RayJob - use shorter names
        job_name = "job-" + str(uuid.uuid4())[:8]
        job_body = {
            "apiVersion": constants.GROUP + "/" + constants.JOB_VERSION,
            "kind": constants.JOB_KIND,
            "metadata": {
                "name": job_name,
                "namespace": namespace,
                "labels": {
                    "app.kubernetes.io/name": job_name,
                    "app.kubernetes.io/managed-by": "kuberay",
                },
            },
            "spec": {
                "entrypoint": "python -c \"import ray; ray.init(); print('Ray job completed successfully')\"",
                "rayCluster": cluster_name,
                "runtime": {
                    "env": [
                        {"name": "MY_POD_IP", "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}}}
                    ]
                },
            },
        }
        created_job = self.job_api.submit_job(job=job_body, k8s_namespace=namespace)
        self.job_api.wait_until_job_finished(job_name, namespace, 120, 10)

        # Delete the RayJob
        self.job_api.delete_job(job_name, namespace)

        # Delete the RayCluster
        self.cluster_api.delete_ray_cluster(cluster_name, namespace)

    def test_submit_ray_job_to_new_cluster(self):
        """Test submit_ray_job_to_new_cluster method."""
        # Create a RayJob with a new RayCluster - use shorter names
        job_name = "job-" + str(uuid.uuid4())[:8]
        namespace = "default"
        cluster_name = "test-" + str(uuid.uuid4())[:8]
        cluster_body = self.director.build_small_cluster(
            name=cluster_name,
            k8s_namespace=namespace,
            labels={"ray.io/cluster": cluster_name}
        )
        job_body = {
            "apiVersion": constants.GROUP + "/" + constants.JOB_VERSION,
            "kind": constants.JOB_KIND,
            "metadata": {
                "name": job_name,
                "namespace": namespace,
                "labels": {
                    "app.kubernetes.io/name": job_name,
                    "app.kubernetes.io/managed-by": "kuberay",
                },
            },
            "spec": {
                "entrypoint": "python -c \"import ray; ray.init(); print('Ray job completed successfully')\"",
                "rayCluster": cluster_name,
                "runtime": {
                    "env": [
                        {"name": "MY_POD_IP", "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}}}
                    ]
                },
            },
        }
        # First create the cluster
        created_cluster = self.cluster_api.create_ray_cluster(
            body=cluster_body, k8s_namespace=namespace
        )
        self.cluster_api.wait_until_ray_cluster_running(cluster_name, namespace, 120, 10)
        
        # Then submit the job
        created_job = self.job_api.submit_job(job=job_body, k8s_namespace=namespace)
        self.job_api.wait_until_job_finished(job_name, namespace, 120, 10)

        # Delete the RayJob
        self.job_api.delete_job(job_name, namespace)

        # Delete the RayCluster
        self.cluster_api.delete_ray_cluster(cluster_name, namespace)


if __name__ == "__main__":
    unittest.main()
