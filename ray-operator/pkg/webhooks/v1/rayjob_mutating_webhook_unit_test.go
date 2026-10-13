package v1

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

func TestRayJobWebhookDefaultsSubmitterAccessLabel(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterNetworkPolicy, true)

	rayJob := &rayv1.RayJob{
		Spec: rayv1.RayJobSpec{
			SubmitterPodTemplate: &corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"existing": "label"}},
			},
		},
	}
	webhook := &RayJobWebhook{RESTMapper: &mockOpenShiftRESTMapperUnit{}}

	require.NoError(t, webhook.Default(context.Background(), rayJob))

	assert.Equal(t, "label", rayJob.Spec.SubmitterPodTemplate.Labels["existing"])
	assert.Equal(t, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD),
		rayJob.Spec.SubmitterPodTemplate.Labels[utils.RayOriginatedFromCRDLabelKey])
}

func TestRayJobWebhookDoesNotDefaultSubmitterAccessLabelOutsideOpenShift(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterNetworkPolicy, true)

	rayJob := &rayv1.RayJob{
		Spec: rayv1.RayJobSpec{
			SubmitterPodTemplate: &corev1.PodTemplateSpec{},
		},
	}
	webhook := &RayJobWebhook{RESTMapper: &mockKubernetesRESTMapper{}}

	require.NoError(t, webhook.Default(context.Background(), rayJob))

	assert.Empty(t, rayJob.Spec.SubmitterPodTemplate.Labels)
}

func TestRayJobWebhookDoesNotDefaultSubmitterAccessLabelForNonK8sJobModes(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterNetworkPolicy, true)

	for _, mode := range []rayv1.JobSubmissionMode{rayv1.SidecarMode, rayv1.HTTPMode, rayv1.InteractiveMode} {
		t.Run(string(mode), func(t *testing.T) {
			rayJob := &rayv1.RayJob{
				Spec: rayv1.RayJobSpec{
					SubmissionMode: mode,
					SubmitterPodTemplate: &corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{}},
					},
				},
			}
			webhook := &RayJobWebhook{RESTMapper: &mockOpenShiftRESTMapperUnit{}}

			require.NoError(t, webhook.Default(context.Background(), rayJob))

			assert.Empty(t, rayJob.Spec.SubmitterPodTemplate.Labels)
		})
	}
}
