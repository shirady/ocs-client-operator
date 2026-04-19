package controller

import (
	"context"

	"github.com/red-hat-storage/ocs-client-operator/pkg/utils"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// ObcCrdReconciler watches the ObjectBucketClaim CRD and restarts the process when its presence
// diverges from startup (so manager cache and ObcReconciler registration stay correct; see cmd/main.go).
type ObcCrdReconciler struct {
	client.Client
	ObcCrdPresentAtStart bool
}

//+kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch

func (r *ObcCrdReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("ObjectBucketClaimCrd").
		For(
			&extv1.CustomResourceDefinition{},
			builder.WithPredicates(
				predicate.NewPredicateFuncs(func(obj client.Object) bool {
					return obj.GetName() == ObjectBucketClaimCrdName
				}),
				// Include delete so we restart when the CRD is removed after startup (parity with former ensureObcWatch).
				utils.EventTypePredicate(true, true, true, false),
			),
		).
		Complete(r)
}

func (r *ObcCrdReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	crd := &extv1.CustomResourceDefinition{}
	crd.Name = ObjectBucketClaimCrdName
	err := r.Get(ctx, client.ObjectKeyFromObject(crd), crd)
	var crdExists bool
	switch {
	case err == nil:
		crdExists = crd.UID != ""
	case kerrors.IsNotFound(err):
		crdExists = false
	default:
		return ctrl.Result{}, err
	}
	utils.AssertEqual(r.ObcCrdPresentAtStart, crdExists, utils.ExitCodeThatShouldRestartTheProcess)
	return ctrl.Result{}, nil
}
