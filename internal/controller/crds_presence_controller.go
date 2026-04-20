package controller

import (
	"context"
	"slices"

	"github.com/red-hat-storage/ocs-client-operator/pkg/utils"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// CrdsWatchedForPresenceRestart lists CRDs whose install/removal must match process startup
// (manager cache and conditional controller registration; see cmd/main.go).
// IMPORTANT - only add the cases where the dynamic watch for the CRD did not match the case.
var CrdsWatchedForPresenceRestart = []string{
	ObjectBucketClaimCrdName,
	// MaintenanceModeCRDName, // TODO
}

type CrdsPresenceReconciler struct {
	client.Client
	AvailableCrds   map[string]bool
	ProcessShutdown func()
}

func (r *CrdsPresenceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("CrdsPresence").
		For(
			&extv1.CustomResourceDefinition{},
			builder.WithPredicates(
				predicate.NewPredicateFuncs(func(obj client.Object) bool {
					return slices.Contains(CrdsWatchedForPresenceRestart, obj.GetName())
				}),
				// Create: CRD installed after start; Delete: CRD removed after start.
				utils.EventTypePredicate(true, false, true, false),
			),
		).
		Complete(r)
}

//+kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch

func (r *CrdsPresenceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	for _, name := range CrdsWatchedForPresenceRestart {
		crd := &extv1.CustomResourceDefinition{}
		crd.Name = name
		if err := r.Get(ctx, client.ObjectKeyFromObject(crd), crd); client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, err
		}
		presentNow := crd.UID != ""
		if r.AvailableCrds[name] != presentNow {
			r.ProcessShutdown()
			return ctrl.Result{}, nil
		}
	}
	return ctrl.Result{}, nil
}
