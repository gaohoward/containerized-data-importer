package clone

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-logr/logr"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cdiv1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"
	cc "kubevirt.io/containerized-data-importer/pkg/controller/common"
	metrics "kubevirt.io/containerized-data-importer/pkg/monitoring/metrics/cdi-cloner"
	"kubevirt.io/containerized-data-importer/pkg/util"
)

// HostClonePhaseName is the name of the host clone phase
const HostClonePhaseName = "HostClone"

// HostClonePhase creates and monitors a dumb clone operation
type HostClonePhase struct {
	Owner             client.Object
	Namespace         string
	SourceName        string
	DesiredClaim      *corev1.PersistentVolumeClaim
	ImmediateBind     bool
	OwnershipLabel    string
	Preallocation     bool
	PriorityClassName string
	Client            client.Client
	Log               logr.Logger
	Recorder          record.EventRecorder
}

var _ Phase = &HostClonePhase{}

var _ StatusReporter = &HostClonePhase{}

var httpClient *http.Client

func init() {
	httpClient = cc.BuildHTTPClient(httpClient)
}

// Name returns the name of the phase
func (p *HostClonePhase) Name() string {
	return HostClonePhaseName
}

// Status returns the phase status
func (p *HostClonePhase) Status(ctx context.Context) (*PhaseStatus, error) {
	result := &PhaseStatus{}
	pvc := &corev1.PersistentVolumeClaim{}
	exists, err := getResource(ctx, p.Client, p.Namespace, p.DesiredClaim.Name, pvc)
	if err != nil {
		return nil, err
	}

	if !exists {
		return result, nil
	}

	result.Annotations = pvc.Annotations

	podName := pvc.Annotations[cc.AnnCloneSourcePod]
	if podName == "" {
		return result, nil
	}

	args := &progressFromClaimArgs{
		Client:       p.Client,
		HTTPClient:   httpClient,
		Claim:        pvc,
		PodNamespace: p.Namespace,
		PodName:      podName,
		OwnerUID:     string(p.Owner.GetUID()),
	}

	progress, err := progressFromClaim(ctx, args)
	if err != nil {
		return nil, err
	}

	result.Progress = progress

	return result, nil
}

// progressFromClaimArgs are the args for ProgressFromClaim
type progressFromClaimArgs struct {
	Client       client.Client
	HTTPClient   *http.Client
	Claim        *corev1.PersistentVolumeClaim
	OwnerUID     string
	PodNamespace string
	PodName      string
}

// progressFromClaim returns the progres
func progressFromClaim(ctx context.Context, args *progressFromClaimArgs) (string, error) {
	// Just set 100.0% if pod is succeeded
	if args.Claim.Annotations[cc.AnnPodPhase] == string(corev1.PodSucceeded) {
		return cc.ProgressDone, nil
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: args.PodNamespace,
			Name:      args.PodName,
		},
	}
	if err := args.Client.Get(ctx, client.ObjectKeyFromObject(pod), pod); err != nil {
		if k8serrors.IsNotFound(err) {
			return "", nil
		}
		return "", err
	}

	// This will only work when the clone source pod is running
	if pod.Status.Phase != corev1.PodRunning {
		return "", nil
	}
	url, err := cc.GetMetricsURL(pod)
	if err != nil {
		return "", err
	}
	if url == "" {
		return "", nil
	}

	// We fetch the clone progress from the clone source pod metrics
	progressReport, err := cc.GetProgressReportFromURL(ctx, url, args.HTTPClient, metrics.CloneProgressMetricName, args.OwnerUID)
	if err != nil {
		return "", err
	}
	if progressReport != "" {
		if f, err := strconv.ParseFloat(progressReport, 64); err == nil {
			return fmt.Sprintf("%.2f%%", f), nil
		}
	}

	return "", nil
}

// Reconcile creates the desired pvc and waits for the operation to complete
func (p *HostClonePhase) Reconcile(ctx context.Context) (*reconcile.Result, error) {
	p.Log.Info("=== reconciling host clone phase", "namespace", p.Namespace, "claim", p.DesiredClaim.Name)
	actualClaim := &corev1.PersistentVolumeClaim{}
	exists, err := getResource(ctx, p.Client, p.Namespace, p.DesiredClaim.Name, actualClaim)
	if err != nil {
		return nil, err
	}

	if !exists {
		p.Log.Info("=== creating host clone PVC(tmp-pvc) as not exist")
		actualClaim, err = p.createClaim(ctx)
		if err != nil {
			return nil, err
		}
	}

	p.Log.Info("checking if clone complete")

	if !p.hostCloneComplete(actualClaim) {
		// requeue to update status
		p.Log.Info("=== host clone not complete, requeuing", "namespace", p.Namespace, "claim", p.DesiredClaim.Name)
		return &reconcile.Result{RequeueAfter: 3 * time.Second}, nil
	}

	p.Log.Info("=== host clone complete", "namespace", p.Namespace, "claim", p.DesiredClaim.Name)

	return nil, nil
}

func (p *HostClonePhase) createClaim(ctx context.Context) (*corev1.PersistentVolumeClaim, error) {

	p.Log.Info("=== creating host clone PVC", "name", p.DesiredClaim.Name, "p:namespace", p.Namespace)

	claim := p.DesiredClaim.DeepCopy()

	claim.Namespace = p.Namespace

	p.Log.Info("=== adding annotations to claim copy")

	cc.AddAnnotation(claim, cc.AnnPreallocationRequested, fmt.Sprintf("%t", p.Preallocation))
	p.Log.Info("adding", "ann key", cc.AnnPreallocationRequested, "value", fmt.Sprintf("%t", p.Preallocation))

	cc.AddAnnotation(claim, cc.AnnOwnerUID, string(p.Owner.GetUID()))
	p.Log.Info("adding", "ann key", cc.AnnOwnerUID, "value", string(p.Owner.GetUID()))

	cc.AddAnnotation(claim, cc.AnnPodRestarts, "0")
	p.Log.Info("adding", "ann key", cc.AnnPodRestarts, "value", "0")

	cc.AddAnnotation(claim, cc.AnnCloneRequest, fmt.Sprintf("%s/%s", p.Namespace, p.SourceName))
	p.Log.Info("adding", "ann key", cc.AnnCloneRequest, "value", fmt.Sprintf("%s/%s", p.Namespace, p.SourceName))

	cc.AddAnnotation(claim, cc.AnnPopulatorKind, cdiv1.VolumeCloneSourceRef)
	p.Log.Info("adding", "ann key", cc.AnnPopulatorKind, "value", cdiv1.VolumeCloneSourceRef)

	cc.AddAnnotation(claim, cc.AnnEventSourceKind, p.Owner.GetObjectKind().GroupVersionKind().Kind)
	p.Log.Info("adding", "ann key", cc.AnnEventSourceKind, "value", p.Owner.GetObjectKind().GroupVersionKind().Kind)

	cc.AddAnnotation(claim, cc.AnnEventSource, fmt.Sprintf("%s/%s", p.Owner.GetNamespace(), p.Owner.GetName()))
	p.Log.Info("adding", "ann key", cc.AnnEventSource, "value", fmt.Sprintf("%s/%s", p.Owner.GetNamespace(), p.Owner.GetName()))

	if p.OwnershipLabel != "" {
		p.Log.Info("adding ownership label", "label", p.OwnershipLabel)
		AddOwnershipLabel(p.OwnershipLabel, claim, p.Owner)
	}
	if p.ImmediateBind {
		p.Log.Info("p.ImmediateBind true, adding annotation", "ann key", cc.AnnImmediateBinding)
		cc.AddAnnotation(claim, cc.AnnImmediateBinding, "")
	}
	if p.PriorityClassName != "" {
		p.Log.Info("priority class name is set, adding annotation", "ann key", cc.AnnPriorityClassName, "value", p.PriorityClassName)
		cc.AddAnnotation(claim, cc.AnnPriorityClassName, p.PriorityClassName)
	}
	p.Log.Info("adding label", "label", cc.LabelExcludeFromVeleroBackup)
	cc.AddLabel(claim, cc.LabelExcludeFromVeleroBackup, "true")

	if myVolumeMode := cc.GetVolumeMode(claim); myVolumeMode == corev1.PersistentVolumeFilesystem {
		// It is possible when the source pvc has VolumMode 'block'
		// and the claim has 'filesystem' in which case the filesystem overhead need to be considered
		sourcePvc := &corev1.PersistentVolumeClaim{}
		sourcePvcKey := client.ObjectKey{Namespace: p.Namespace, Name: p.SourceName}

		p.Log.Info("finding source pvc for size checking", "key", sourcePvcKey)

		if err := p.Client.Get(ctx, sourcePvcKey, sourcePvc); err != nil {
			p.Log.Info("=== failed to get source PVC", "pvc", sourcePvcKey, "error", err)
			return nil, err
		}

		realSourcePvcSizeRequest := sourcePvc.Spec.Resources.Requests[corev1.ResourceStorage]

		usableSpace, err := getUsableSpace(ctx, p.Client, claim)
		if err != nil {
			p.Log.Info("=== failed to get usable space", "pvc", claim, "error", err)
			return nil, err
		}
		if usableSpace.Cmp(realSourcePvcSizeRequest) < 0 {
			p.Log.Info("=== not enough usable space", "pvc", claim, "usable", usableSpace, "requested", realSourcePvcSizeRequest)
			if newUsableSpace, err := cc.InflateSizeWithOverhead(ctx, p.Client, realSourcePvcSizeRequest.Value(), &claim.Spec); err != nil {
				p.Log.Info("=== failed to inflate size", "pvc", claim, "error", err)
				return nil, err
			} else {
				p.Log.Info("=== Using new inflated size for pvc", "pvc", claim, "new size", newUsableSpace)
				claim.Spec.Resources.Requests[corev1.ResourceStorage] = newUsableSpace
			}
		}
	}

	p.Log.Info("=== go creating claim", "claim", claim)
	if err := p.Client.Create(ctx, claim); err != nil {
		p.Log.Info("=== failed to create claim, go check quota before return", "claim", claim, "error", err)
		checkQuotaExceeded(p.Recorder, p.Owner, err)
		return nil, err
	}

	p.Log.Info("=== successfully created claim", "claim", *claim)

	return claim, nil
}

func (p *HostClonePhase) hostCloneComplete(pvc *corev1.PersistentVolumeClaim) bool {
	// this is awfully lame
	// both the upload controller and clone controller update the PVC status to succeeded
	// but only the clone controller will set the preallocation annotation
	// so we have to wait for that
	p.Log.Info("=== Check host clone completeness", "preallocation", p.Preallocation, "preall applied", pvc.Annotations[cc.AnnPreallocationApplied])
	if p.Preallocation && pvc.Annotations[cc.AnnPreallocationApplied] != "true" {
		p.Log.Info("=== we are not complete yet, so will reconcile again")
		return false
	}
	p.Log.Info("=== the preallocation is good, now checking pod phase annotation", "key", cc.AnnPodPhase, "pod phase", pvc.Annotations[cc.AnnPodPhase])
	return pvc.Annotations[cc.AnnPodPhase] == string(cdiv1.Succeeded)
}

// copied from clone-controller.
// Todo: move it to a util package to share
func getUsableSpace(ctx context.Context, c client.Client, pvc *corev1.PersistentVolumeClaim) (resource.Quantity, error) {
	sizeRequest := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	volumeMode := util.ResolveVolumeMode(pvc.Spec.VolumeMode)

	if volumeMode == corev1.PersistentVolumeFilesystem {
		fsOverhead, err := cc.GetFilesystemOverheadForStorageClass(ctx, c, pvc.Spec.StorageClassName)
		if err != nil {
			return resource.Quantity{}, err
		}
		fsOverheadFloat, _ := strconv.ParseFloat(string(fsOverhead), 64)
		usableSpaceRaw := util.GetUsableSpace(fsOverheadFloat, sizeRequest.Value())

		return *resource.NewScaledQuantity(usableSpaceRaw, 0), nil
	}

	return sizeRequest, nil
}
