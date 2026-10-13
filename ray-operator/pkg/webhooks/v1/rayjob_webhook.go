package v1

import (
	"context"

	routev1 "github.com/openshift/api/route/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

var rayJobLog = logf.Log.WithName("rayjob-resource")

// SetupRayJobWebhookWithManager registers the webhook for RayJob in the manager.
func SetupRayJobWebhookWithManager(mgr ctrl.Manager) error {
	rayJobWebhook := &RayJobWebhook{RESTMapper: mgr.GetRESTMapper()}
	return ctrl.NewWebhookManagedBy(mgr, &rayv1.RayJob{}).
		WithDefaulter(rayJobWebhook).
		WithValidator(rayJobWebhook).
		Complete()
}

type RayJobWebhook struct {
	RESTMapper meta.RESTMapper
}

//+kubebuilder:webhook:path=/mutate-ray-io-v1-rayjob,mutating=true,failurePolicy=fail,sideEffects=None,groups=ray.io,resources=rayjobs,verbs=create;update,versions=v1,name=mrayjob.kb.io,admissionReviewVersions=v1
//+kubebuilder:webhook:path=/validate-ray-io-v1-rayjob,mutating=false,failurePolicy=fail,sideEffects=None,groups=ray.io,resources=rayjobs,verbs=create;update,versions=v1,name=vrayjob.kb.io,admissionReviewVersions=v1

var (
	_ admission.Defaulter[*rayv1.RayJob] = &RayJobWebhook{}
	_ admission.Validator[*rayv1.RayJob] = &RayJobWebhook{}
)

// Default adds the label used by the OpenShift RayCluster network policy
// default to an explicitly configured RayJob submitter template.
func (w *RayJobWebhook) Default(_ context.Context, rayJob *rayv1.RayJob) error {
	if !w.isOpenShift() || !features.Enabled(features.RayClusterNetworkPolicy) ||
		(rayJob.Spec.SubmissionMode != "" && rayJob.Spec.SubmissionMode != rayv1.K8sJobMode) ||
		rayJob.Spec.SubmitterPodTemplate == nil {
		return nil
	}

	if rayJob.Spec.SubmitterPodTemplate.Labels == nil {
		rayJob.Spec.SubmitterPodTemplate.Labels = make(map[string]string)
	}
	rayJob.Spec.SubmitterPodTemplate.Labels[utils.RayOriginatedFromCRDLabelKey] = utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)
	return nil
}

// ValidateCreate implements admission.Validator so a webhook will be registered for the type
func (w *RayJobWebhook) ValidateCreate(_ context.Context, rayJob *rayv1.RayJob) (admission.Warnings, error) {
	rayJobLog.Info("validate create", "name", rayJob.Name)
	return nil, w.validateRayJob(rayJob)
}

// ValidateUpdate implements admission.Validator so a webhook will be registered for the type
func (w *RayJobWebhook) ValidateUpdate(_ context.Context, _ *rayv1.RayJob, rayJob *rayv1.RayJob) (admission.Warnings, error) {
	rayJobLog.Info("validate update", "name", rayJob.Name)
	return nil, w.validateRayJob(rayJob)
}

// ValidateDelete implements admission.Validator so a webhook will be registered for the type
func (w *RayJobWebhook) ValidateDelete(_ context.Context, _ *rayv1.RayJob) (admission.Warnings, error) {
	return nil, nil
}

func (w *RayJobWebhook) validateRayJob(rayJob *rayv1.RayJob) error {
	var allErrs field.ErrorList

	if err := utils.ValidateRayJobMetadata(rayJob.ObjectMeta); err != nil {
		allErrs = append(allErrs, field.Invalid(field.NewPath("metadata").Child("name"), rayJob.Name, err.Error()))
	}

	if len(allErrs) == 0 {
		return nil
	}

	return apierrors.NewInvalid(
		schema.GroupKind{Group: "ray.io", Kind: "RayJob"},
		rayJob.Name, allErrs)
}

func (w *RayJobWebhook) isOpenShift() bool {
	if w.RESTMapper == nil {
		return false
	}
	gvk := routev1.GroupVersion.WithKind("Route")
	_, err := w.RESTMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	return err == nil
}
