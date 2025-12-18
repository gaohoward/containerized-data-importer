/*
 * This file is part of the CDI project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright 2023 Red Hat, Inc.
 *
 */

package webhooks

import (
	"context"
	"encoding/json"

	admissionv1 "k8s.io/api/admission/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"sigs.k8s.io/controller-runtime/pkg/client"
	yaml "sigs.k8s.io/yaml/goyaml.v2"

	"kubevirt.io/containerized-data-importer/pkg/common"
	dvc "kubevirt.io/containerized-data-importer/pkg/controller/datavolume"
)

type pvcMutatingWebhook struct {
	cachedClient client.Client
}

func (wh *pvcMutatingWebhook) Admit(ar admissionv1.AdmissionReview) *admissionv1.AdmissionResponse {

	klog.Info("[pvc-mutate] Admit()")

	if ar.Request.Operation != admissionv1.Create {
		return allowedAdmissionResponse()
	}

	if err := validatePvcResource(ar); err != nil {
		return toAdmissionResponseError(err)
	}

	pvc := &v1.PersistentVolumeClaim{}
	if err := json.Unmarshal(ar.Request.Object.Raw, &pvc); err != nil {
		return toAdmissionResponseError(err)
	}

	klog.Infof("[pvc-mutate] mutating pvc %s in ns %s", pvc.Name, pvc.Namespace)

	// Note the webhook LabelSelector should not pass us such pvcs
	if pvc.Labels[common.PvcApplyStorageProfileLabel] != "true" {
		klog.Warningf("Got PVC %s/%s which was not labeled for rendering", pvc.Namespace, pvc.Name)
		return allowedAdmissionResponse()
	}

	pvcCpy := pvc.DeepCopy()
	klog.Infof("[pvc-mutate] calling dvc.RederPvc()...")
	if err := dvc.RenderPvc(context.TODO(), wh.cachedClient, pvcCpy); err != nil {
		return toAdmissionResponseError(err)
	}

	bytes, err := yaml.Marshal(pvcCpy)
	if err != nil {
		klog.Infof("[pvc-mutate] error marshal pvc %v", err)
	}

	klog.Infof("[pvc-mutate] patching rendered pvc ---\n%s\n---", string(bytes))
	return toPatchResponse(pvc, pvcCpy)
}
