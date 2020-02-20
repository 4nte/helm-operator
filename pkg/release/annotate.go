package release

import (
	"fmt"

	"github.com/fluxcd/flux/pkg/resource"
	"github.com/ghodss/yaml"
	"github.com/go-kit/kit/log"

	"helm.sh/helm/v3/pkg/releaseutil"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/util/retry"

	"github.com/fluxcd/helm-operator/pkg/helm"
)

// AntecedentAnnotation is an annotation on a resource indicating that
// the cause of that resource is a HelmRelease. We use this rather than
// the `OwnerReference` type built into Kubernetes as this does not
// allow cross-namespace references by design. The value is expected to
// be a serialised `resource.ID`.
const AntecedentAnnotation = "helm.fluxcd.io/antecedent"

// annotatedWithResourceID determines if the resources of the given
// `helm.Release` are annotated with the antecedent annotation with
// a value that equals to the given `resource.ID`. It returns a
// boolean indicating the presence of the annotation with the right
// `resource.ID` and a string with the value of the annotation, or
// an error.
//
// If there are no errors and no annotations were found either, it
// assumes the release has been installed manually and we want to
// take over.
func annotatedWithResourceID(kubeConfig *rest.Config, rel *helm.Release, resourceID resource.ID) (bool, string, error) {
	client, err  := dynamic.NewForConfig(kubeConfig)
	if err != nil {
		return false, "", err
	}
	restMap, err := buildDiscoveryRestMapper(kubeConfig)
	if err != nil {
		return false, "", err
	}

	objs := releaseManifestToUnstructured(rel.Manifest, log.NewNopLogger())
	for _, obj := range objs {
		mapping, err := restMap.RESTMapping(obj.GroupVersionKind().GroupKind(), obj.GroupVersionKind().Version)
		if err != nil {
			continue
		}

		if obj.GetNamespace() == "" {
			obj.SetNamespace(rel.Namespace)
		}

		{
			var res *unstructured.Unstructured
			var err error
			wait.ExponentialBackoff(retry.DefaultBackoff, func() (bool, error) {
				res, err = client.Resource(mapping.Resource).Namespace(obj.GetNamespace()).Get(obj.GetName(), metav1.GetOptions{})
				// All these errors indicate a transient error that should
				// be retried.
				if net.IsConnectionReset(err) || errors.IsInternalError(err) || errors.IsTimeout(err) || errors.IsTooManyRequests(err) {
					return false, nil
				}
				// Checks for a Retry-After header, the presence of this
				// header is an explicit signal we should retry.
				if _, shouldRetry := errors.SuggestsClientDelay(err); shouldRetry {
					return false, nil
				}
				if err != nil {
					return false, err
				}
				return true, nil
			})

			if err != nil {
				return false, "", err
			}

			if v, ok := res.GetAnnotations()[AntecedentAnnotation]; ok {
				return v == resourceID.String(), v, nil
			}
		}
	}
	return true, "", nil
}

// annotateWithResourceID annotates all of the resources in the given
// `helm.Release` with a antecedent annotation holding the provided
// `resource.ID`.
func annotateWithResourceID(logger log.Logger, kubeConfig *rest.Config, rel *helm.Release, resourceID resource.ID) error {
	client, err  := dynamic.NewForConfig(kubeConfig)
	if err != nil {
		return err
	}
	restMap, err := buildDiscoveryRestMapper(kubeConfig)
	if err != nil {
		return err
	}

	annotation := []byte(`{"metadata":{"annotations":{"`+AntecedentAnnotation+`":"`+resourceID.String()+`"}}}`)
	objs := releaseManifestToUnstructured(rel.Manifest, logger)
	for _, obj := range objs {
		mapping, err := restMap.RESTMapping(obj.GroupVersionKind().GroupKind(), obj.GroupVersionKind().Version)
		if err != nil {
			logger.Log("error", fmt.Sprintf("failed to get REST mapping for group version kind: %#v", obj.GroupVersionKind()), "err", err)
			continue
		}

		if obj.GetNamespace() == "" {
			obj.SetNamespace(rel.Namespace)
		}

		if _, err := client.Resource(mapping.Resource).Namespace(obj.GetNamespace()).Patch(obj.GetName(), types.MergePatchType, annotation, metav1.PatchOptions{}); err != nil {
			logger.Log("error", fmt.Sprintf("failed to mark resource '%s/%s' with antecedent annotation", obj.GetKind(), obj.GetName()), "err", err)
		}
	}
	return nil
}

// releaseManifestToUnstructured turns a string containing YAML
// manifests into an array of Unstructured objects.
func releaseManifestToUnstructured(manifest string, logger log.Logger) []unstructured.Unstructured {
	manifests := releaseutil.SplitManifests(manifest)
	var objs []unstructured.Unstructured
	for _, manifest := range manifests {
		var u unstructured.Unstructured

		if err := yaml.Unmarshal([]byte(manifest), &u); err != nil {
			continue
		}

		// Helm charts may include list kinds, we are only interested in
		// the items on those lists.
		if u.IsList() {
			l, err := u.ToList()
			if err != nil {
				logger.Log("err", err)
				continue
			}
			objs = append(objs, l.Items...)
			continue
		}

		objs = append(objs, u)
	}
	return objs
}

func buildDiscoveryRestMapper(kubeConfig *rest.Config) (meta.RESTMapper, error) {
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(kubeConfig)
	if err != nil {
		return nil, err
	}
	groupResources, err := restmapper.GetAPIGroupResources(discoveryClient)
	if err != nil {
		return nil, err
	}
	return restmapper.NewDiscoveryRESTMapper(groupResources), nil
}