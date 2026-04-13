// Cache scope test — exercises the same Kubernetes list API surface the manager cache uses
// for ConfigMaps and Secrets:
//   - all objects in the operator namespace
//   - all objects with label app=noobaa in every namespace
//
// Run (from repo root):
//
//	export KUBECONFIG=~/.kube/config
//	export OPERATOR_NAMESPACE=openshift-storage-client   # or your operator ns
//	go run ./hack/cache-scope-test
//
// Or explicitly:
//
//	go run ./hack/cache-scope-test -namespace openshift-storage-client -kubeconfig "$KUBECONFIG"
//
// Expected: exit 0, printed counts, no errors. If RBAC denies list/watch, the program fails
// with the API error (fix ClusterRole or use an admin kubeconfig for debugging).

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const noobaaLabelSelector = "app=noobaa"

func main() {
	kubeconfig := flag.String("kubeconfig", os.Getenv("KUBECONFIG"), "path to kubeconfig (empty: try in-cluster config)")
	opNs := flag.String("namespace", os.Getenv("OPERATOR_NAMESPACE"), "operator namespace (OPERATOR_NAMESPACE)")
	verbose := flag.Bool("verbose", false, "print every object namespace/name")
	flag.Parse()

	if *opNs == "" {
		fmt.Fprintln(os.Stderr, "error: set -namespace or OPERATOR_NAMESPACE to the operator deployment namespace")
		os.Exit(2)
	}

	ctx := context.Background()
	cfg, err := loadRESTConfig(*kubeconfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error loading kubeconfig: %v\n", err)
		os.Exit(1)
	}

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error creating clientset: %v\n", err)
		os.Exit(1)
	}

	// 1) Operator namespace: unfiltered lists (matches cache.Config with LabelSelector: labels.Everything()).
	cmOp, err := clientset.CoreV1().ConfigMaps(*opNs).List(ctx, metav1.ListOptions{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error listing ConfigMaps in operator namespace %q: %v\n", *opNs, err)
		os.Exit(1)
	}
	secOp, err := clientset.CoreV1().Secrets(*opNs).List(ctx, metav1.ListOptions{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error listing Secrets in operator namespace %q: %v\n", *opNs, err)
		os.Exit(1)
	}

	sel, err := labels.Parse(noobaaLabelSelector)
	if err != nil {
		fmt.Fprintf(os.Stderr, "internal error parsing selector: %v\n", err)
		os.Exit(1)
	}
	listOpts := metav1.ListOptions{LabelSelector: sel.String()}

	// 2) All namespaces: label-selected lists (matches cache.AllNamespaces + app=noobaa).
	cmAll, err := clientset.CoreV1().ConfigMaps("").List(ctx, listOpts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error listing ConfigMaps cluster-wide with %s: %v\n", noobaaLabelSelector, err)
		os.Exit(1)
	}
	secAll, err := clientset.CoreV1().Secrets("").List(ctx, listOpts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error listing Secrets cluster-wide with %s: %v\n", noobaaLabelSelector, err)
		os.Exit(1)
	}

	fmt.Println("cache-scope-test: list API checks passed")
	fmt.Printf("operator namespace: %q\n", *opNs)
	fmt.Printf("  ConfigMaps (all): %d\n", len(cmOp.Items))
	fmt.Printf("  Secrets    (all): %d\n", len(secOp.Items))
	fmt.Printf("cluster-wide with label %q:\n", noobaaLabelSelector)
	fmt.Printf("  ConfigMaps: %d\n", len(cmAll.Items))
	fmt.Printf("  Secrets:    %d\n", len(secAll.Items))

	// Distinct keys in the union of both list patterns (same logical set the dual informers cover).
	fmt.Printf("distinct ConfigMap keys (operator-ns all ∪ cluster %s): %d\n", noobaaLabelSelector, len(unionConfigMapKeys(cmOp.Items, cmAll.Items)))
	fmt.Printf("distinct Secret keys    (operator-ns all ∪ cluster %s): %d\n", noobaaLabelSelector, len(unionSecretKeys(secOp.Items, secAll.Items)))

	if *verbose {
		printObjects("ConfigMaps in operator namespace", cmOp.Items)
		printObjects("Secrets in operator namespace", secOp.Items)
		printObjects("ConfigMaps with "+noobaaLabelSelector, cmAll.Items)
		printObjects("Secrets with "+noobaaLabelSelector, secAll.Items)
	}
}

func loadRESTConfig(explicitPath string) (*rest.Config, error) {
	if explicitPath != "" {
		return clientcmd.BuildConfigFromFlags("", explicitPath)
	}
	// Try default loading rules (~/.kube/config, KUBECONFIG env).
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	overrides := &clientcmd.ConfigOverrides{}
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides)
	if cfg, err := cc.ClientConfig(); err == nil {
		return cfg, nil
	}
	return rest.InClusterConfig()
}

func unionConfigMapKeys(opItems, labeledAllNs []corev1.ConfigMap) map[string]struct{} {
	out := make(map[string]struct{})
	for i := range opItems {
		cm := &opItems[i]
		out[objectKey(cm.Namespace, cm.Name)] = struct{}{}
	}
	for i := range labeledAllNs {
		cm := &labeledAllNs[i]
		out[objectKey(cm.Namespace, cm.Name)] = struct{}{}
	}
	return out
}

func unionSecretKeys(opItems, labeledAllNs []corev1.Secret) map[string]struct{} {
	out := make(map[string]struct{})
	for i := range opItems {
		s := &opItems[i]
		out[objectKey(s.Namespace, s.Name)] = struct{}{}
	}
	for i := range labeledAllNs {
		s := &labeledAllNs[i]
		out[objectKey(s.Namespace, s.Name)] = struct{}{}
	}
	return out
}

func objectKey(ns, name string) string { return ns + "/" + name }

func printObjects(title string, items interface{}) {
	fmt.Printf("\n--- %s ---\n", title)
	switch xs := items.(type) {
	case []corev1.ConfigMap:
		lines := make([]string, 0, len(xs))
		for i := range xs {
			lines = append(lines, fmt.Sprintf("  %s/%s", xs[i].Namespace, xs[i].Name))
		}
		sort.Strings(lines)
		fmt.Println(strings.Join(lines, "\n"))
	case []corev1.Secret:
		lines := make([]string, 0, len(xs))
		for i := range xs {
			lines = append(lines, fmt.Sprintf("  %s/%s", xs[i].Namespace, xs[i].Name))
		}
		sort.Strings(lines)
		fmt.Println(strings.Join(lines, "\n"))
	}
}
