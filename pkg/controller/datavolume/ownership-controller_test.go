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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cdiv1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"
	"kubevirt.io/containerized-data-importer/pkg/common"
	. "kubevirt.io/containerized-data-importer/pkg/controller/common"
	featuregates "kubevirt.io/containerized-data-importer/pkg/feature-gates"
)

var (
	ownershipLog = logf.Log.WithName("datavolume-ownership-controller-test")
)

var _ = Describe("DataVolume Ownership Controller Tests", func() {
	var (
		reconciler *OwnershipReconciler
	)

	AfterEach(func() {
		if reconciler != nil {
			reconciler = nil
		}
	})

	Describe("Reconcile", func() {
		It("Should return nil when DataVolume does not exist", func() {
			reconciler = createOwnershipReconciler()
			result, err := reconciler.Reconcile(context.TODO(), reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "non-existent-dv", Namespace: metav1.NamespaceDefault},
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))
		})

		It("Should return nil when DataVolume exists", func() {
			dv := newDataVolumeForOwnershipTest("test-dv")
			reconciler = createOwnershipReconciler(dv)
			result, err := reconciler.Reconcile(context.TODO(), reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "test-dv", Namespace: metav1.NamespaceDefault},
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))
		})
	})

	Describe("getDataVolumeRequestsFromPvc", func() {
		It("Should return DataVolume name when PVC has ownerReference", func() {
			dv := newDataVolumeForOwnershipTest("test-dv")
			pvc := newPvcForOwnershipTest("test-pvc", dv)

			requests := getDataVolumeRequestsFromPvc(pvc)
			Expect(requests).To(HaveLen(1))
			Expect(requests[0].Name).To(Equal("test-dv"))
			Expect(requests[0].Namespace).To(Equal(metav1.NamespaceDefault))
		})

		It("Should return DataVolume name when PVC has AnnOwnedByDataVolume annotation", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: metav1.NamespaceDefault,
					Annotations: map[string]string{
						AnnOwnedByDataVolume: "default/test-dv",
					},
				},
			}

			requests := getDataVolumeRequestsFromPvc(pvc)
			Expect(requests).To(HaveLen(1))
			Expect(requests[0].Name).To(Equal("test-dv"))
			Expect(requests[0].Namespace).To(Equal(metav1.NamespaceDefault))
		})

		It("Should return DataVolume name when PVC has AnnPopulatedFor annotation", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: metav1.NamespaceDefault,
					Annotations: map[string]string{
						AnnPopulatedFor: "test-dv",
					},
				},
			}

			requests := getDataVolumeRequestsFromPvc(pvc)
			Expect(requests).To(HaveLen(1))
			Expect(requests[0].Name).To(Equal("test-dv"))
			Expect(requests[0].Namespace).To(Equal(metav1.NamespaceDefault))
		})

		It("Should deduplicate DataVolume names from multiple sources", func() {
			dv := newDataVolumeForOwnershipTest("test-dv")
			pvc := newPvcForOwnershipTest("test-pvc", dv)
			if pvc.Annotations == nil {
				pvc.Annotations = make(map[string]string)
			}
			pvc.Annotations[AnnOwnedByDataVolume] = "default/test-dv"
			pvc.Annotations[AnnPopulatedFor] = "test-dv"

			requests := getDataVolumeRequestsFromPvc(pvc)
			Expect(requests).To(HaveLen(1))
			Expect(requests[0].Name).To(Equal("test-dv"))
		})

		It("Should return empty list when PVC has no DataVolume ownership", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: metav1.NamespaceDefault,
				},
			}

			requests := getDataVolumeRequestsFromPvc(pvc)
			Expect(requests).To(BeEmpty())
		})
	})

	Describe("hasDataVolumeOwnership", func() {
		It("Should return true when PVC has ownerReference to DataVolume", func() {
			dv := newDataVolumeForOwnershipTest("test-dv")
			pvc := newPvcForOwnershipTest("test-pvc", dv)

			Expect(hasDataVolumeOwnership(pvc)).To(BeTrue())
		})

		It("Should return true when PVC has AnnOwnedByDataVolume annotation", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: metav1.NamespaceDefault,
					Annotations: map[string]string{
						AnnOwnedByDataVolume: "default/test-dv",
					},
				},
			}

			Expect(hasDataVolumeOwnership(pvc)).To(BeTrue())
		})

		It("Should return true when PVC has AnnPopulatedFor annotation", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: metav1.NamespaceDefault,
					Annotations: map[string]string{
						AnnPopulatedFor: "test-dv",
					},
				},
			}

			Expect(hasDataVolumeOwnership(pvc)).To(BeTrue())
		})

		It("Should return false when PVC has no DataVolume ownership", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: metav1.NamespaceDefault,
				},
			}

			Expect(hasDataVolumeOwnership(pvc)).To(BeFalse())
		})
	})

	Describe("ownershipChanged", func() {
		It("Should return true when ownerReference added", func() {
			dv := newDataVolumeForOwnershipTest("test-dv")
			oldPvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: metav1.NamespaceDefault,
				},
			}
			newPvc := newPvcForOwnershipTest("test-pvc", dv)

			Expect(ownershipChanged(oldPvc, newPvc)).To(BeTrue())
		})

		It("Should return true when ownerReference removed", func() {
			dv := newDataVolumeForOwnershipTest("test-dv")
			oldPvc := newPvcForOwnershipTest("test-pvc", dv)
			newPvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: metav1.NamespaceDefault,
				},
			}

			Expect(ownershipChanged(oldPvc, newPvc)).To(BeTrue())
		})

		It("Should return true when ownerReference UID changed", func() {
			dv1 := newDataVolumeForOwnershipTest("test-dv-1")
			dv2 := newDataVolumeForOwnershipTest("test-dv-2")
			oldPvc := newPvcForOwnershipTest("test-pvc", dv1)
			newPvc := newPvcForOwnershipTest("test-pvc", dv2)

			Expect(ownershipChanged(oldPvc, newPvc)).To(BeTrue())
		})

		It("Should return true when AnnOwnedByDataVolume annotation added", func() {
			oldPvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: metav1.NamespaceDefault,
				},
			}
			newPvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: metav1.NamespaceDefault,
					Annotations: map[string]string{
						AnnOwnedByDataVolume: "default/test-dv",
					},
				},
			}

			Expect(ownershipChanged(oldPvc, newPvc)).To(BeTrue())
		})

		It("Should return true when AnnPopulatedFor annotation changed", func() {
			oldPvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: metav1.NamespaceDefault,
					Annotations: map[string]string{
						AnnPopulatedFor: "dv-1",
					},
				},
			}
			newPvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: metav1.NamespaceDefault,
					Annotations: map[string]string{
						AnnPopulatedFor: "dv-2",
					},
				},
			}

			Expect(ownershipChanged(oldPvc, newPvc)).To(BeTrue())
		})

		It("Should return false when ownership did not change", func() {
			dv := newDataVolumeForOwnershipTest("test-dv")
			oldPvc := newPvcForOwnershipTest("test-pvc", dv)
			newPvc := newPvcForOwnershipTest("test-pvc", dv)

			Expect(ownershipChanged(oldPvc, newPvc)).To(BeFalse())
		})

		It("Should return false when only non-ownership fields changed", func() {
			dv := newDataVolumeForOwnershipTest("test-dv")
			oldPvc := newPvcForOwnershipTest("test-pvc", dv)
			newPvc := newPvcForOwnershipTest("test-pvc", dv)
			if newPvc.Labels == nil {
				newPvc.Labels = make(map[string]string)
			}
			newPvc.Labels["new-label"] = "value"

			Expect(ownershipChanged(oldPvc, newPvc)).To(BeFalse())
		})
	})
})

func createOwnershipReconciler(objects ...interface{}) *OwnershipReconciler {
	objs := []client.Object{}
	for _, obj := range objects {
		if o, ok := obj.(client.Object); ok {
			objs = append(objs, o)
		}
	}

	// Add empty CDI config
	cdiConfig := MakeEmptyCDIConfigSpec(common.ConfigName)
	cdiConfig.Status = cdiv1.CDIConfigStatus{
		DefaultPodResourceRequirements: &corev1.ResourceRequirements{},
	}
	objs = append(objs, cdiConfig)

	s := scheme.Scheme
	_ = cdiv1.AddToScheme(s)

	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
	rec := record.NewFakeRecorder(10)

	r := &OwnershipReconciler{
		ReconcilerBase: ReconcilerBase{
			client:       cl,
			scheme:       s,
			log:          ownershipLog,
			recorder:     rec,
			featureGates: featuregates.NewFeatureGates(cl),
			installerLabels: map[string]string{
				common.AppKubernetesPartOfLabel:  "testing",
				common.AppKubernetesVersionLabel: "v0.0.0-tests",
			},
		},
	}
	return r
}

func newDataVolumeForOwnershipTest(name string) *cdiv1.DataVolume {
	return &cdiv1.DataVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
			UID:       types.UID(name + "-uid"),
		},
		Spec: cdiv1.DataVolumeSpec{
			Source: &cdiv1.DataVolumeSource{
				HTTP: &cdiv1.DataVolumeSourceHTTP{
					URL: "http://example.com/image.iso",
				},
			},
			PVC: &corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: *ptr.To(resource.MustParse("1Gi")),
					},
				},
			},
		},
	}
}

func newPvcForOwnershipTest(name string, dv *cdiv1.DataVolume) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(dv, cdiv1.SchemeGroupVersion.WithKind("DataVolume")),
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: *ptr.To(resource.MustParse("1Gi")),
				},
			},
		},
	}
}
