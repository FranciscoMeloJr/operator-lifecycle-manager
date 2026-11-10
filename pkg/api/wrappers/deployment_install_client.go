//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 deployment_install_client.go InstallStrategyDeploymentInterface
package wrappers

import (
	"context"
	"strings"

	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/operator-framework/operator-lifecycle-manager/pkg/lib/operatorclient"
	"github.com/operator-framework/operator-lifecycle-manager/pkg/lib/operatorlister"
	"github.com/operator-framework/operator-lifecycle-manager/pkg/lib/ownerutil"
)

var ErrNilObject = errors.New("Bad object supplied: <nil>")

type InstallStrategyDeploymentInterface interface {
	CreateRole(role *rbacv1.Role) (*rbacv1.Role, error)
	CreateRoleBinding(roleBinding *rbacv1.RoleBinding) (*rbacv1.RoleBinding, error)
	EnsureServiceAccount(serviceAccount *corev1.ServiceAccount, owner ownerutil.Owner) (*corev1.ServiceAccount, error)
	CreateDeployment(deployment *appsv1.Deployment) (*appsv1.Deployment, error)
	CreateOrUpdateDeployment(deployment *appsv1.Deployment) (*appsv1.Deployment, error)
	DeleteDeployment(name string) error
	GetServiceAccountByName(serviceAccountName string) (*corev1.ServiceAccount, error)
	FindAnyDeploymentsMatchingNames(depNames []string) ([]*appsv1.Deployment, error)
	FindAnyDeploymentsMatchingLabels(label labels.Selector) ([]*appsv1.Deployment, error)
	GetOpClient() operatorclient.ClientInterface
	GetOpLister() operatorlister.OperatorLister
}

type InstallStrategyDeploymentClientForNamespace struct {
	opClient  operatorclient.ClientInterface
	opLister  operatorlister.OperatorLister
	Namespace string
}

var _ InstallStrategyDeploymentInterface = &InstallStrategyDeploymentClientForNamespace{}

func NewInstallStrategyDeploymentClient(opClient operatorclient.ClientInterface, opLister operatorlister.OperatorLister, namespace string) InstallStrategyDeploymentInterface {
	return &InstallStrategyDeploymentClientForNamespace{
		opClient:  opClient,
		opLister:  opLister,
		Namespace: namespace,
	}
}

func (c *InstallStrategyDeploymentClientForNamespace) GetOpClient() operatorclient.ClientInterface {
	return c.opClient
}

func (c *InstallStrategyDeploymentClientForNamespace) GetOpLister() operatorlister.OperatorLister {
	return c.opLister
}

func (c *InstallStrategyDeploymentClientForNamespace) CreateRole(role *rbacv1.Role) (*rbacv1.Role, error) {
	return c.opClient.CreateRole(role)
}

func (c *InstallStrategyDeploymentClientForNamespace) CreateRoleBinding(roleBinding *rbacv1.RoleBinding) (*rbacv1.RoleBinding, error) {
	return c.opClient.CreateRoleBinding(roleBinding)
}

func (c *InstallStrategyDeploymentClientForNamespace) EnsureServiceAccount(serviceAccount *corev1.ServiceAccount, owner ownerutil.Owner) (*corev1.ServiceAccount, error) {
	if serviceAccount == nil {
		return nil, ErrNilObject
	}

	foundAccount, err := c.opLister.CoreV1().ServiceAccountLister().ServiceAccounts(c.Namespace).Get(serviceAccount.Name)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, errors.Wrap(err, "checking for existing serviceacccount failed")
	}

	// create if not found
	if err != nil && apierrors.IsNotFound(err) {
		serviceAccount.SetNamespace(c.Namespace)
		createdAccount, err := c.opClient.CreateServiceAccount(serviceAccount)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, errors.Wrap(err, "creating serviceacccount failed")
		}
		if apierrors.IsAlreadyExists(err) {
			return serviceAccount, nil
		}
		return createdAccount, nil
	}

	// if found, ensure ownerreferences
	if ownerutil.IsOwnedBy(foundAccount, owner) {
		return foundAccount, nil
	}
	// set owner if missing
	ownerutil.AddNonBlockingOwner(foundAccount, owner)
	return c.opClient.UpdateServiceAccount(foundAccount)
}

func (c *InstallStrategyDeploymentClientForNamespace) CreateDeployment(deployment *appsv1.Deployment) (*appsv1.Deployment, error) {
	return c.opClient.CreateDeployment(deployment)
}

func (c *InstallStrategyDeploymentClientForNamespace) DeleteDeployment(name string) error {
	foregroundDelete := metav1.DeletePropagationForeground // cascading delete
	// Note(tflannag): See https://bugzilla.redhat.com/show_bug.cgi?id=1939294.
	immediate := int64(1)
	immediateForegroundDelete := &metav1.DeleteOptions{GracePeriodSeconds: &immediate, PropagationPolicy: &foregroundDelete}
	return c.opClient.DeleteDeployment(c.Namespace, name, immediateForegroundDelete)
}

func (c *InstallStrategyDeploymentClientForNamespace) CreateOrUpdateDeployment(deployment *appsv1.Deployment) (*appsv1.Deployment, error) {
	liveDeployment, err := c.opClient.GetDeployment(deployment.Namespace, deployment.Name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, err
		}
		created, err := c.CreateDeployment(deployment)
		if err != nil {
			return nil, err
		}
		return created, err
	}

	// Preserve foreign annotations and labels from the live deployment that OLM doesn't manage.
	// This allows user-initiated operations like "oc rollout restart" to work correctly.
	// See: https://github.com/operator-framework/operator-lifecycle-manager/issues/3392
	mergeForeignMetadata(deployment, liveDeployment)

	return c.opClient.KubernetesInterface().AppsV1().Deployments(deployment.GetNamespace()).Update(context.TODO(), deployment, metav1.UpdateOptions{})
}

// mergeForeignMetadata merges annotations and labels from the live deployment into the desired
// deployment if they are not claimed/managed by OLM. This preserves user-added metadata like
// kubectl.kubernetes.io/restartedAt (added by "oc rollout restart") while ensuring OLM-managed
// metadata always comes from the CSV.
func mergeForeignMetadata(desired, live *appsv1.Deployment) {
	// Preserve foreign annotations on Deployment object metadata
	if desired.Annotations == nil {
		desired.Annotations = make(map[string]string)
	}
	for k, v := range live.Annotations {
		if !isOLMClaimedAnnotation(k) {
			if _, exists := desired.Annotations[k]; !exists {
				desired.Annotations[k] = v
			}
		}
	}

	// Preserve foreign labels on Deployment object metadata
	if desired.Labels == nil {
		desired.Labels = make(map[string]string)
	}
	for k, v := range live.Labels {
		if !isOLMClaimedLabel(k) {
			if _, exists := desired.Labels[k]; !exists {
				desired.Labels[k] = v
			}
		}
	}

	// Preserve foreign annotations on Pod Template
	if desired.Spec.Template.Annotations == nil {
		desired.Spec.Template.Annotations = make(map[string]string)
	}
	for k, v := range live.Spec.Template.Annotations {
		if !isOLMClaimedAnnotation(k) {
			if _, exists := desired.Spec.Template.Annotations[k]; !exists {
				desired.Spec.Template.Annotations[k] = v
			}
		}
	}

	// Preserve foreign labels on Pod Template
	if desired.Spec.Template.Labels == nil {
		desired.Spec.Template.Labels = make(map[string]string)
	}
	for k, v := range live.Spec.Template.Labels {
		if !isOLMClaimedLabel(k) {
			if _, exists := desired.Spec.Template.Labels[k]; !exists {
				desired.Spec.Template.Labels[k] = v
			}
		}
	}
}

// isOLMClaimedAnnotation returns true if the annotation key is managed/claimed by OLM.
// OLM-claimed annotations are always taken from the CSV and not preserved from live.
func isOLMClaimedAnnotation(key string) bool {
	// OLM-owned annotation prefixes
	if strings.HasPrefix(key, "olm.") ||
		strings.HasPrefix(key, "operatorframework.io/") ||
		strings.HasPrefix(key, "operators.coreos.com/") {
		return true
	}

	// System annotations that should not be preserved
	if key == "kubectl.kubernetes.io/last-applied-configuration" ||
		key == "deployment.kubernetes.io/revision" {
		return true
	}

	return false
}

// isOLMClaimedLabel returns true if the label key is managed/claimed by OLM.
// OLM-claimed labels are always taken from the CSV and not preserved from live.
func isOLMClaimedLabel(key string) bool {
	// OLM-owned label prefixes and specific labels
	if strings.HasPrefix(key, "olm.") ||
		strings.HasPrefix(key, "operatorframework.io/") ||
		strings.HasPrefix(key, "operators.coreos.com/") {
		return true
	}

	return false
}

func (c *InstallStrategyDeploymentClientForNamespace) GetServiceAccountByName(serviceAccountName string) (*corev1.ServiceAccount, error) {
	return c.opLister.CoreV1().ServiceAccountLister().ServiceAccounts(c.Namespace).Get(serviceAccountName)
}

func (c *InstallStrategyDeploymentClientForNamespace) FindAnyDeploymentsMatchingNames(depNames []string) ([]*appsv1.Deployment, error) {
	var deployments []*appsv1.Deployment
	for _, depName := range depNames {
		fetchedDep, err := c.opLister.AppsV1().DeploymentLister().Deployments(c.Namespace).Get(depName)
		if err == nil {
			deployments = append(deployments, fetchedDep)
		} else {
			// Any errors other than !exists are propagated up
			if !apierrors.IsNotFound(err) {
				return deployments, err
			}
		}
	}
	return deployments, nil
}

func (c *InstallStrategyDeploymentClientForNamespace) FindAnyDeploymentsMatchingLabels(label labels.Selector) ([]*appsv1.Deployment, error) {
	deployments, err := c.opLister.AppsV1().DeploymentLister().Deployments(c.Namespace).List(label)
	if err != nil {
		return nil, err
	}

	return deployments, nil
}
