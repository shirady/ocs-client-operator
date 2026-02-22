package controller

import (
	"context"

	nbv1 "github.com/noobaa/noobaa-operator/v5/pkg/apis/noobaa/v1alpha1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// OBCReconciler reconciles a ObjectBucketClaim object
type OBCReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// SetupWithManager sets up the controller with the Manager.
func (r *OBCReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Only reconcile on Create and Delete events (not on Update).
	createOrDeleteOnly := predicate.Funcs{
		CreateFunc: func(event.CreateEvent) bool { return true },
		UpdateFunc: func(event.UpdateEvent) bool { return false },
		DeleteFunc: func(event.DeleteEvent) bool { return true },
	}

	return ctrl.NewControllerManagedBy(mgr).
		Named("OBC").
		For(
			&nbv1.ObjectBucketClaim{},
			builder.WithPredicates(createOrDeleteOnly),
		).
		Complete(r)
}

//+kubebuilder:rbac:groups=objectbucket.io,resources=objectbucketclaims,verbs=get;list;watch

func (r *OBCReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithName("OBC")

	obc := &nbv1.ObjectBucketClaim{}
	err := r.Get(ctx, req.NamespacedName, obc)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Info("OBC deleted", "namespace", req.Namespace, "name", req.Name)
			return reconcile.Result{}, nil
		}
		log.Error(err, "failed to get ObjectBucketClaim")
		return reconcile.Result{}, err
	}

	if obc.DeletionTimestamp != nil {
		log.Info("OBC deleted", "namespace", obc.Namespace, "name", obc.Name)
		return reconcile.Result{}, nil
	}

	log.Info("OBC created", "namespace", obc.Namespace, "name", obc.Name)
	return reconcile.Result{}, nil
}
