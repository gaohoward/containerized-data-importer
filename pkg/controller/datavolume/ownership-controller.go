/*
Copyright 2024 The CDI Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package datavolume

import (
	"context"

	"github.com/go-logr/logr"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	cdiv1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"
	cc "kubevirt.io/containerized-data-importer/pkg/controller/common"
	featuregates "kubevirt.io/containerized-data-importer/pkg/feature-gates"
)

const (
	ownershipControllerName = "datavolume-ownership-controller"
)

// OwnershipReconciler reconciles ownership changes for DataVolumes watching PVC updates
type OwnershipReconciler struct {
	ReconcilerBase
}

// NewOwnershipController creates a new instance of the datavolume ownership-change controller
func NewOwnershipController(
	ctx context.Context,
	mgr manager.Manager,
	log logr.Logger,
	installerLabels map[string]string,
) (controller.Controller, error) {
	client := mgr.GetClient()
	reconciler := &OwnershipReconciler{
		ReconcilerBase: ReconcilerBase{
			client:               client,
			scheme:               mgr.GetScheme(),
			log:                  log.WithName(ownershipControllerName),
			recorder:             mgr.GetEventRecorderFor(ownershipControllerName),
			featureGates:         featuregates.NewFeatureGates(client),
			installerLabels:      installerLabels,
			shouldUpdateProgress: false,
		},
	}

	ownershipController, err := controller.New(ownershipControllerName, mgr, controller.Options{
		MaxConcurrentReconciles: 1,
		Reconciler:              reconciler,
	})
	if err != nil {
		return nil, err
	}

	if err := addOwnershipControllerWatches(mgr, ownershipController); err != nil {
		return nil, err
	}

	return ownershipController, nil
}

func addOwnershipControllerWatches(mgr manager.Manager, ownershipController controller.Controller) error {
	// Watch DataVolumes and enqueue the DV itself
	if err := ownershipController.Watch(source.Kind(mgr.GetCache(), &cdiv1.DataVolume{}, handler.TypedEnqueueRequestsFromMapFunc[*cdiv1.DataVolume](
		func(ctx context.Context, dv *cdiv1.DataVolume) []reconcile.Request {
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: dv.Namespace, Name: dv.Name}}}
		}),
	)); err != nil {
		return err
	}

	// Watch PVCs for ownership changes
	pvcPredicate := predicate.TypedFuncs[*corev1.PersistentVolumeClaim]{
		CreateFunc: func(e event.TypedCreateEvent[*corev1.PersistentVolumeClaim]) bool {
			return hasDataVolumeOwnership(e.Object)
		},
		UpdateFunc: func(e event.TypedUpdateEvent[*corev1.PersistentVolumeClaim]) bool {
			return ownershipChanged(e.ObjectOld, e.ObjectNew)
		},
		DeleteFunc: func(e event.TypedDeleteEvent[*corev1.PersistentVolumeClaim]) bool {
			return hasDataVolumeOwnership(e.Object)
		},
		GenericFunc: func(e event.TypedGenericEvent[*corev1.PersistentVolumeClaim]) bool {
			return hasDataVolumeOwnership(e.Object)
		},
	}

	if err := ownershipController.Watch(source.Kind(mgr.GetCache(), &corev1.PersistentVolumeClaim{}, handler.TypedEnqueueRequestsFromMapFunc[*corev1.PersistentVolumeClaim](
		func(ctx context.Context, pvc *corev1.PersistentVolumeClaim) []reconcile.Request {
			return getDataVolumeRequestsFromPvc(pvc)
		}),
		pvcPredicate,
	)); err != nil {
		return err
	}

	return nil
}

// Reconcile loop for the ownership change controller
func (r *OwnershipReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := r.log.WithValues("DataVolume", req.NamespacedName)

	// Fetch the DataVolume
	dv := &cdiv1.DataVolume{}
	if err := r.client.Get(ctx, req.NamespacedName, dv); err != nil {
		// If not found, no-op; this is normal during deletion
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}

	log.V(3).Info("Reconciling DataVolume ownership change", "DataVolume", dv.Name)

	// No-op: this controller's purpose is to enqueue DataVolumes when their owned
	// PVCs' ownership changes. The actual DV reconciliation logic is handled by
	// the operation-specific controllers (import, upload, clone, etc.).
	return reconcile.Result{}, nil
}

// hasDataVolumeOwnership returns true if the PVC is owned by a DataVolume
func hasDataVolumeOwnership(pvc *corev1.PersistentVolumeClaim) bool {
	// Check ownerReference
	owner := metav1.GetControllerOf(pvc)
	if owner != nil && owner.Kind == "DataVolume" {
		return true
	}

	// Check annotation
	if hasAnnOwnedByDataVolume(pvc) {
		return true
	}

	// Check AnnPopulatedFor for cross-namespace references
	if pvc.GetAnnotations()[cc.AnnPopulatedFor] != "" {
		return true
	}

	return false
}

// ownershipChanged returns true if ownership metadata changed between old and new PVC
func ownershipChanged(oldPvc, newPvc *corev1.PersistentVolumeClaim) bool {
	oldOwner := metav1.GetControllerOf(oldPvc)
	newOwner := metav1.GetControllerOf(newPvc)

	// Check if ownerReference changed
	oldHasOwner := oldOwner != nil && oldOwner.Kind == "DataVolume"
	newHasOwner := newOwner != nil && newOwner.Kind == "DataVolume"

	if oldHasOwner != newHasOwner {
		return true
	}

	if oldHasOwner && newHasOwner && oldOwner.UID != newOwner.UID {
		return true
	}

	// Check if AnnOwnedByDataVolume annotation changed
	oldAnnValue := oldPvc.GetAnnotations()[AnnOwnedByDataVolume]
	newAnnValue := newPvc.GetAnnotations()[AnnOwnedByDataVolume]

	if oldAnnValue != newAnnValue {
		return true
	}

	// Check if AnnPopulatedFor annotation changed
	oldPopulatedFor := oldPvc.GetAnnotations()[cc.AnnPopulatedFor]
	newPopulatedFor := newPvc.GetAnnotations()[cc.AnnPopulatedFor]

	if oldPopulatedFor != newPopulatedFor {
		return true
	}

	return false
}

// getDataVolumeRequestsFromPvc returns reconcile requests for DataVolumes that own the PVC
func getDataVolumeRequestsFromPvc(pvc *corev1.PersistentVolumeClaim) []reconcile.Request {
	var requests []reconcile.Request
	dvNames := getDataVolumeNamesFromPvc(pvc)

	for _, dvName := range dvNames {
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: pvc.Namespace,
				Name:      dvName,
			},
		})
	}

	return requests
}

// getDataVolumeNamesFromPvc returns unique DataVolume names that own the PVC
func getDataVolumeNamesFromPvc(pvc *corev1.PersistentVolumeClaim) []string {
	seen := make(map[string]bool)
	var names []string

	// Check ownerReference
	owner := metav1.GetControllerOf(pvc)
	if owner != nil && owner.Kind == "DataVolume" {
		if !seen[owner.Name] {
			names = append(names, owner.Name)
			seen[owner.Name] = true
		}
	}

	// Check AnnOwnedByDataVolume annotation
	if hasAnnOwnedByDataVolume(pvc) {
		namespace, name, err := getAnnOwnedByDataVolume(pvc)
		if err == nil && namespace == pvc.Namespace {
			if !seen[name] {
				names = append(names, name)
				seen[name] = true
			}
		}
	}

	// Check AnnPopulatedFor annotation
	if populatedFor := pvc.GetAnnotations()[cc.AnnPopulatedFor]; populatedFor != "" {
		if !seen[populatedFor] {
			names = append(names, populatedFor)
			seen[populatedFor] = true
		}
	}

	return names
}
