package v1

import (
	"context"
	"os"
	"reflect"

	routev1 "github.com/openshift/api/route/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

// RayClusterDefaulter mutates RayClusters
type RayClusterDefaulter struct {
	RESTMapper meta.RESTMapper
}

//+kubebuilder:webhook:path=/mutate-ray-io-v1-raycluster,mutating=true,failurePolicy=fail,sideEffects=None,groups=ray.io,resources=rayclusters,verbs=create;update,versions=v1,name=mraycluster.kb.io,admissionReviewVersions=v1

var _ admission.Defaulter[*rayv1.RayCluster] = &RayClusterDefaulter{}

// Default implements webhook.CustomDefaulter
func (d *RayClusterDefaulter) Default(_ context.Context, rayCluster *rayv1.RayCluster) error {
	rayClusterLog.Info("default", "name", rayCluster.Name)

	// Initialize annotations map if nil
	if rayCluster.Annotations == nil {
		rayCluster.Annotations = make(map[string]string)
	}

	// Set the secure network annotation based on platform
	if d.isOpenShift() {
		// Keep the legacy auth switch enabled independently of the optional
		// mTLS and NetworkPolicy features.
		rayCluster.Annotations[utils.EnableSecureTrustedNetworkAnnotationKey] = "true"
		if features.Enabled(features.RayClusterMTLS) {
			if rayCluster.Spec.TLSOptions == nil {
				enabled := true
				rayCluster.Spec.TLSOptions = &rayv1.TLSOptions{Enabled: &enabled}
			}
		}
		if features.Enabled(features.RayClusterNetworkPolicy) {
			applyOpenShiftNetworkPolicyDefaults(rayCluster)
		}
		rayClusterLog.Info("setting OpenShift RayCluster defaults", "name", rayCluster.Name, "namespace", rayCluster.Namespace)

		// STRICT ENFORCEMENT: Always disable basic Route/Ingress creation on OpenShift
		// This enforces Gateway API access only - no exceptions
		// Authentication controller will create HTTPRoute for Gateway API authentication
		// This prevents direct Route access and enforces centralized authentication via Gateway
		falseValue := false
		if rayCluster.Spec.HeadGroupSpec.EnableIngress != nil && *rayCluster.Spec.HeadGroupSpec.EnableIngress {
			rayClusterLog.Info("overriding user-specified enableIngress from true to false to enforce Gateway-only access",
				"name", rayCluster.Name, "namespace", rayCluster.Namespace)
		}
		rayCluster.Spec.HeadGroupSpec.EnableIngress = &falseValue
	} else {
		rayCluster.Annotations[utils.EnableSecureTrustedNetworkAnnotationKey] = "false"
	}

	return nil
}

// applyOpenShiftNetworkPolicyDefaults applies the OpenShift ingress contract
// to every RayCluster admitted while the NetworkPolicy feature gate is enabled.
// It is deliberately kept in the webhook path; the legacy controller uses the
// migration helper for pre-existing annotation-based RayClusters.
func applyOpenShiftNetworkPolicyDefaults(cluster *rayv1.RayCluster) {
	if cluster.Spec.NetworkPolicy == nil {
		cluster.Spec.NetworkPolicy = &rayv1.NetworkPolicyConfig{}
	}

	mode := rayv1.NetworkPolicyDenyAllIngress
	cluster.Spec.NetworkPolicy.Mode = &mode
	if cluster.Spec.NetworkPolicy.Head == nil {
		cluster.Spec.NetworkPolicy.Head = &rayv1.NetworkPolicyRules{}
	}

	tcp := corev1.ProtocolTCP
	port := func(value int32) networkingv1.NetworkPolicyPort {
		p := intstr.FromInt32(value)
		return networkingv1.NetworkPolicyPort{Protocol: &tcp, Port: &p}
	}
	appendIngress := func(rule networkingv1.NetworkPolicyIngressRule) {
		for _, existing := range cluster.Spec.NetworkPolicy.Head.IngressRules {
			if reflect.DeepEqual(existing, rule) {
				return
			}
		}
		cluster.Spec.NetworkPolicy.Head.IngressRules = append(cluster.Spec.NetworkPolicy.Head.IngressRules, rule)
	}

	// Same-namespace dashboard and client access.
	appendIngress(networkingv1.NetworkPolicyIngressRule{
		From:  []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{}}},
		Ports: []networkingv1.NetworkPolicyPort{port(8265), port(10001)},
	})

	// RayJob submitter access for standalone and clusterSelector RayClusters.
	appendIngress(networkingv1.NetworkPolicyIngressRule{
		From: []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{
				utils.RayOriginatedFromCRDLabelKey: utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD),
			},
		}}},
		Ports: []networkingv1.NetworkPolicyPort{port(8265)},
	})

	// The authenticated proxy is deliberately reachable from any namespace.
	appendIngress(networkingv1.NetworkPolicyIngressRule{
		Ports: []networkingv1.NetworkPolicyPort{port(8443)},
	})

	namespaces := []string{}
	if namespace := os.Getenv("APPLICATION_NAMESPACE"); namespace != "" {
		namespaces = append(namespaces, namespace)
	}
	if namespace := os.Getenv("POD_NAMESPACE"); namespace != "" {
		namespaces = append(namespaces, namespace)
	}
	if len(namespaces) == 0 {
		namespaces = []string{"redhat-ods-applications", "opendatahub"}
	}
	operatorPeer := networkingv1.NetworkPolicyPeer{
		PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{utils.KubernetesApplicationNameLabelKey: utils.ApplicationName}},
		NamespaceSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
			Key: corev1.LabelMetadataName, Operator: metav1.LabelSelectorOpIn, Values: namespaces,
		}}},
	}
	appendIngress(networkingv1.NetworkPolicyIngressRule{
		From:  []networkingv1.NetworkPolicyPeer{operatorPeer},
		Ports: []networkingv1.NetworkPolicyPort{port(8265), port(10001)},
	})

	gatewayNamespace := os.Getenv("GATEWAY_NAMESPACE")
	if gatewayNamespace == "" {
		gatewayNamespace = "openshift-ingress"
	}
	appendIngress(networkingv1.NetworkPolicyIngressRule{
		From: []networkingv1.NetworkPolicyPeer{{NamespaceSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
			Key: corev1.LabelMetadataName, Operator: metav1.LabelSelectorOpIn, Values: []string{gatewayNamespace},
		}}}}},
		Ports: []networkingv1.NetworkPolicyPort{port(8265)},
	})
}

// isOpenShift checks if the cluster is running on OpenShift
func (d *RayClusterDefaulter) isOpenShift() bool {
	gvk := routev1.GroupVersion.WithKind("Route")
	_, err := d.RESTMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	return err == nil
}

// SetupRayClusterDefaulterWithManager registers the defaulting webhook for RayCluster
func SetupRayClusterDefaulterWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &rayv1.RayCluster{}).
		WithDefaulter(&RayClusterDefaulter{
			RESTMapper: mgr.GetRESTMapper(),
		}).
		Complete()
}
