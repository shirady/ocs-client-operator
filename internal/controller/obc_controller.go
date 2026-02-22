package controller

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	nbv1 "github.com/noobaa/noobaa-operator/v5/pkg/apis/noobaa/v1alpha1"
	"github.com/red-hat-storage/ocs-client-operator/api/v1alpha1"
	"github.com/red-hat-storage/ocs-client-operator/pkg/utils"
	providerClient "github.com/red-hat-storage/ocs-operator/services/provider/api/v4/client"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	operatorObcFinalizer = "ocs-client-operator.ocs.openshift.io/obc"
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
//+kubebuilder:rbac:groups=ocs.openshift.io,resources=storageclients,verbs=get;list;watch

func (r *OBCReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithName("OBC")

	obc := &nbv1.ObjectBucketClaim{}
	err := r.Get(ctx, req.NamespacedName, obc)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Info("OBC deleted", "namespace", req.Namespace, "name", req.Name)
			if err := r.notifyObcDeleted(ctx, log, req.NamespacedName); err != nil {
				log.Error(err, "failed to notify provider of OBC deletion")
				return reconcile.Result{}, err
			}
			return reconcile.Result{}, nil
		}
		log.Error(err, "failed to get ObjectBucketClaim")
		return reconcile.Result{}, err
	}

	if !obc.GetDeletionTimestamp().IsZero() {
		log.Info("OBC deleted", "namespace", obc.Namespace, "name", obc.Name)
		if err := r.notifyObcDeleted(ctx, log, types.NamespacedName{Namespace: obc.Namespace, Name: obc.Name}); err != nil {
			log.Error(err, "failed to notify provider of OBC deletion")
			return reconcile.Result{}, err
		}
		if controllerutil.RemoveFinalizer(obc, operatorObcFinalizer) {
			log.Info("removing finalizer from OBC.", "OBC", obc.Name)
			if err := r.Update(ctx, obc); err != nil {
				log.Info("Failed to remove finalizer from OBC", "OBC", obc.Name)
				return reconcile.Result{}, fmt.Errorf("failed to remove finalizer from OBC: %v", err)
			}
		}
		return reconcile.Result{}, nil
	}

	log.Info("OBC created", "namespace", obc.Namespace, "name", obc.Name)
	if controllerutil.AddFinalizer(obc, operatorObcFinalizer) {
		log.Info("Finalizer not found for OBC. Adding finalizer.", "OBC", obc.Name)
		if err := r.Update(ctx, obc); err != nil {
			log.Info("Failed to add finalizer to OBC", "OBC", obc.Name, obc.Namespace)
			return reconcile.Result{}, fmt.Errorf("failed to add finalizer to OBC: %v", err)
		}
	}
	if err := r.notifyObcCreated(ctx, log, obc); err != nil {
		log.Error(err, "failed to notify provider of OBC creation")
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

// getStorageClientForNotify is a temporary function
// until I can get the ownerReferences from the storageclass
func (r *OBCReconciler) getStorageClientForNotify(ctx context.Context) (*v1alpha1.StorageClient, error) {
	list := &v1alpha1.StorageClientList{}
	if err := r.List(ctx, list); err != nil {
		return nil, fmt.Errorf("list StorageClients: %w", err)
	}
	for i := range list.Items {
		sc := &list.Items[i]
		if sc.Status.ConsumerID != "" && sc.Spec.StorageProviderEndpoint != "" {
			return sc, nil
		}
	}
	return nil, fmt.Errorf("no StorageClient with ConsumerID and StorageProviderEndpoint found")
}

func (r *OBCReconciler) notifyObcCreated(ctx context.Context, log logr.Logger, obc *nbv1.ObjectBucketClaim) error {
	storageClient, err := r.getStorageClientForNotify(ctx)
	if err != nil {
		return err
	}
	pc, err := NewProviderClientForStorageClient(ctx, storageClient)
	if err != nil {
		return fmt.Errorf("create provider client: %w", err)
	}
	defer pc.Close()
	_, err = pc.NotifyObcCreated(ctx, storageClient.Status.ConsumerID, obc)
	if err != nil {
		return fmt.Errorf("NotifyObcCreated: %w", err)
	}
	log.Info("notified provider of OBC creation", "namespace", obc.Namespace, "name", obc.Name)
	return nil
}

func (r *OBCReconciler) notifyObcDeleted(ctx context.Context, log logr.Logger, nn types.NamespacedName) error {
	storageClient, err := r.getStorageClientForNotify(ctx)
	if err != nil {
		return err
	}
	pc, err := NewProviderClientForStorageClient(ctx, storageClient)
	if err != nil {
		return fmt.Errorf("create provider client: %w", err)
	}
	defer pc.Close()
	_, err = pc.NotifyObcDeleted(ctx, storageClient.Status.ConsumerID, nn)
	if err != nil {
		return fmt.Errorf("NotifyObcDeleted: %w", err)
	}
	log.Info("notified provider of OBC deletion", "namespace", nn.Namespace, "name", nn.Name)
	return nil
}

// NewProviderClientForStorageClient creates an OCS provider gRPC client for the given StorageClient.
func NewProviderClientForStorageClient(ctx context.Context, sc *v1alpha1.StorageClient) (*providerClient.OCSProviderClient, error) {
	pc, err := providerClient.NewProviderClient(ctx, sc.Spec.StorageProviderEndpoint, utils.OcsClientTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to create provider client with endpoint %v: %w", sc.Spec.StorageProviderEndpoint, err)
	}
	return pc, nil
}
