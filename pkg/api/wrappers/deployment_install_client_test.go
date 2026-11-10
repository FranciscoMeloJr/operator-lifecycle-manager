package wrappers

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/diff"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/operator-framework/api/pkg/operators/v1alpha1"
	listerfakes "github.com/operator-framework/operator-lifecycle-manager/pkg/fakes/client-go/listers"
	"github.com/operator-framework/operator-lifecycle-manager/pkg/lib/operatorclient/operatorclientmocks"
	"github.com/operator-framework/operator-lifecycle-manager/pkg/lib/operatorlister/operatorlisterfakes"
)

var (
	Controller         = false
	BlockOwnerDeletion = false
	WakeupInterval     = 5 * time.Second
)

func ownerReferenceFromCSV(csv *v1alpha1.ClusterServiceVersion) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion:         v1alpha1.SchemeGroupVersion.String(),
		Kind:               v1alpha1.ClusterServiceVersionKind,
		Name:               csv.GetName(),
		UID:                csv.GetUID(),
		Controller:         &Controller,
		BlockOwnerDeletion: &BlockOwnerDeletion,
	}
}

func TestEnsureServiceAccount(t *testing.T) {
	testErr := errors.New("NaNaNaNaN") // used to ensure exact error returned
	mockOwner := v1alpha1.ClusterServiceVersion{
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha1.ClusterServiceVersionKind,
			APIVersion: v1alpha1.ClusterServiceVersionAPIVersion,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "csv-owner",
			Namespace: "test-namespace",
		},
	}
	type state struct {
		namespace                  string
		existingServiceAccount     *corev1.ServiceAccount
		getServiceAccountError     error
		createServiceAccountResult *corev1.ServiceAccount
		createServiceAccountError  error
		updateServiceAccountResult *corev1.ServiceAccount
		updateServiceAccountError  error
	}
	type input struct {
		serviceAccountName     string
		serviceAccount         *corev1.ServiceAccount
		serviceAccountToUpdate *corev1.ServiceAccount
	}
	type expect struct {
		returnedServiceAccount *corev1.ServiceAccount
		returnedError          error
	}

	tests := []struct {
		name    string
		subname string
		state   state
		input   input
		expect  expect
	}{
		{
			name:    "Bad ServiceAccount",
			subname: "nil value",
			expect: expect{
				returnedError: ErrNilObject,
			},
		},
		{
			name:    "ServiceAccount already exists, owned by CSV",
			subname: "returns existing SA when successfully fetched via Kubernetes API",
			state: state{
				namespace: "test-namespace",
				existingServiceAccount: &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-service-account",
						Labels: map[string]string{
							"test": "existing-service-account-found",
						},
						OwnerReferences: []metav1.OwnerReference{
							ownerReferenceFromCSV(&mockOwner),
						},
					},
				},
				getServiceAccountError:    nil,
				createServiceAccountError: nil,
			},
			input: input{
				serviceAccountName: "test-service-account",
				serviceAccount: &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-service-account",
					},
				},
			},
			expect: expect{
				returnedServiceAccount: &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-service-account",
						Labels: map[string]string{
							"test": "existing-service-account-found",
						},
						OwnerReferences: []metav1.OwnerReference{
							ownerReferenceFromCSV(&mockOwner),
						},
					},
				},
				returnedError: nil,
			},
		},
		{
			name:    "ServiceAccount already exists, not owned by CSV",
			subname: "returns existing SA when successfully fetched via Kubernetes API",
			state: state{
				namespace: "test-namespace",
				existingServiceAccount: &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-service-account",
						Namespace: "test-namespace",
						Labels: map[string]string{
							"test": "existing-service-account-found",
						},
					},
				},
				updateServiceAccountResult: &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-service-account",
						Labels: map[string]string{
							"test": "existing-service-account-found",
						},
						OwnerReferences: []metav1.OwnerReference{
							ownerReferenceFromCSV(&mockOwner),
						},
					},
				},
				getServiceAccountError:    nil,
				createServiceAccountError: nil,
				updateServiceAccountError: nil,
			},
			input: input{
				serviceAccountName: "test-service-account",
				serviceAccount: &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-service-account",
					},
				},
				serviceAccountToUpdate: &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-service-account",
						Namespace: "test-namespace",
						Labels: map[string]string{
							"test": "existing-service-account-found",
						},
						OwnerReferences: []metav1.OwnerReference{
							ownerReferenceFromCSV(&mockOwner),
						},
					},
				},
			},
			expect: expect{
				returnedServiceAccount: &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-service-account",
						Labels: map[string]string{
							"test": "existing-service-account-found",
						},
						OwnerReferences: []metav1.OwnerReference{
							ownerReferenceFromCSV(&mockOwner),
						},
					},
				},
				returnedError: nil,
			},
		},
		{
			name:    "ServiceAccount already exists, not owned by CSV, update fails",
			subname: "returns existing SA when successfully fetched via Kubernetes API",
			state: state{
				namespace: "test-namespace",
				existingServiceAccount: &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-service-account",
						Namespace: "test-namespace",
						Labels: map[string]string{
							"test": "existing-service-account-found",
						},
					},
				},
				updateServiceAccountResult: nil,
				getServiceAccountError:     nil,
				createServiceAccountError:  nil,
				updateServiceAccountError:  testErr,
			},
			input: input{
				serviceAccountName: "test-service-account",
				serviceAccount: &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-service-account",
					},
				},
				serviceAccountToUpdate: &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-service-account",
						Namespace: "test-namespace",
						Labels: map[string]string{
							"test": "existing-service-account-found",
						},
						OwnerReferences: []metav1.OwnerReference{
							ownerReferenceFromCSV(&mockOwner),
						},
					},
				},
			},
			expect: expect{
				returnedServiceAccount: nil,
				returnedError:          testErr,
			},
		},
		{
			name:    "ServiceAccount already exists",
			subname: "returns SA unmodified when fails to create it due to it already existing",
			state: state{
				namespace: "test-namespace",
				existingServiceAccount: &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-service-account",
						Namespace: "test-namespace",
						Labels: map[string]string{
							"test": "existing-service-account-create-conflict",
						},
						OwnerReferences: []metav1.OwnerReference{
							ownerReferenceFromCSV(&mockOwner),
						},
					},
				},
				getServiceAccountError: nil,
				createServiceAccountError: apierrors.NewAlreadyExists(
					corev1.Resource("serviceaccounts"), "test-service-account"),
			},
			input: input{
				serviceAccountName: "test-service-account",
				serviceAccount: &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-service-account",
					},
				},
			},
			expect: expect{
				returnedServiceAccount: &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-service-account",
						Namespace: "test-namespace",
						Labels: map[string]string{
							"test": "existing-service-account-create-conflict",
						},
						OwnerReferences: []metav1.OwnerReference{
							ownerReferenceFromCSV(&mockOwner),
						},
					},
				},
				returnedError: nil,
			},
		},
		{
			name:    "ServiceAccount doesn't already exist",
			subname: "creates SA when no errors or existing SAs found",
			state: state{
				namespace: "test-namespace",
				createServiceAccountResult: &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-service-account",
						Labels: map[string]string{
							"test": "successfully-created-serviceaccount",
						},
						OwnerReferences: []metav1.OwnerReference{
							ownerReferenceFromCSV(&mockOwner),
						},
					},
				},
				createServiceAccountError: nil,
				getServiceAccountError:    apierrors.NewNotFound(corev1.Resource("serviceaccounts"), "test-service-account"),
			},
			input: input{
				serviceAccountName: "test-service-account",
				serviceAccount: &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-service-account",
					},
				},
			},
			expect: expect{
				returnedServiceAccount: &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-service-account",
						Labels: map[string]string{
							"test": "successfully-created-serviceaccount",
						},
						OwnerReferences: []metav1.OwnerReference{
							ownerReferenceFromCSV(&mockOwner),
						},
					},
				},
				returnedError: nil,
			},
		},
		{
			name:    "ServiceAccount doesn't already exist",
			subname: "creates SA successfully after getting NotFound error trying to fetch it",
			state: state{
				namespace: "test-namespace",
				getServiceAccountError: apierrors.NewNotFound(
					corev1.Resource("serviceaccounts"), "test-service-account"),
				createServiceAccountResult: &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-service-account",
						Labels: map[string]string{
							"test": "successfully-created-serviceaccount-notfound-error",
						},
						OwnerReferences: []metav1.OwnerReference{
							ownerReferenceFromCSV(&mockOwner),
						},
					},
				},
				createServiceAccountError: nil,
			},
			input: input{
				serviceAccountName: "test-service-account",
				serviceAccount: &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-service-account",
					},
				},
			},
			expect: expect{
				returnedServiceAccount: &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-service-account",
						Labels: map[string]string{
							"test": "successfully-created-serviceaccount-notfound-error",
						},
						OwnerReferences: []metav1.OwnerReference{
							ownerReferenceFromCSV(&mockOwner),
						},
					},
				},
				returnedError: nil,
			},
		},
		{
			name:    "Unknown errors",
			subname: "returns unknown errors received trying to fetch SA from the kubernetes API",
			state: state{
				namespace:                 "test-namespace",
				getServiceAccountError:    testErr,
				createServiceAccountError: nil,
			},
			input: input{
				serviceAccountName: "test-service-account",
				serviceAccount: &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-service-account",
						OwnerReferences: []metav1.OwnerReference{
							ownerReferenceFromCSV(&mockOwner),
						},
					},
				},
			},
			expect: expect{
				returnedError: testErr,
			},
		},
		{
			name:    "Unknown errors",
			subname: "returns unknown errors received trying to create SA",
			state: state{
				namespace: "test-namespace",
				getServiceAccountError: apierrors.NewNotFound(
					corev1.Resource("serviceaccounts"), "test-service-account"),
				createServiceAccountError: testErr,
			},
			input: input{
				serviceAccountName: "test-service-account",
				serviceAccount: &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-service-account",
						OwnerReferences: []metav1.OwnerReference{
							ownerReferenceFromCSV(&mockOwner),
						},
					},
				},
			},
			expect: expect{
				returnedError: testErr,
			},
		},
	}

	for _, tt := range tests {
		testName := fmt.Sprintf("%s: %s", tt.name, tt.subname)
		t.Run(testName, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockOpClient := operatorclientmocks.NewMockClientInterface(ctrl)
			fakeLister := &operatorlisterfakes.FakeOperatorLister{}
			fakeCoreV1Lister := &operatorlisterfakes.FakeCoreV1Lister{}
			fakeServiceAccountLister := &listerfakes.FakeServiceAccountLister{}
			fakeServiceAccountNamespacedLister := &listerfakes.FakeServiceAccountNamespaceLister{}
			fakeServiceAccountNamespacedLister.GetReturns(tt.state.existingServiceAccount, tt.state.getServiceAccountError)
			fakeServiceAccountLister.ServiceAccountsReturns(fakeServiceAccountNamespacedLister)
			fakeCoreV1Lister.ServiceAccountListerReturns(fakeServiceAccountLister)
			fakeLister.CoreV1Returns(fakeCoreV1Lister)

			client := NewInstallStrategyDeploymentClient(mockOpClient, fakeLister, tt.state.namespace)

			mockOpClient.EXPECT().
				CreateServiceAccount(tt.input.serviceAccount).
				Return(tt.state.createServiceAccountResult, tt.state.createServiceAccountError).
				AnyTimes()

			mockOpClient.EXPECT().
				UpdateServiceAccount(tt.input.serviceAccountToUpdate).
				Return(tt.state.updateServiceAccountResult, tt.state.updateServiceAccountError).
				AnyTimes()

			sa, err := client.EnsureServiceAccount(tt.input.serviceAccount, &mockOwner)

			require.True(t, equality.Semantic.DeepEqual(tt.expect.returnedServiceAccount, sa),
				"Resources do not match <expected, actual>: %s",
				diff.Diff(tt.expect.returnedServiceAccount, sa))

			require.EqualValues(t, tt.expect.returnedError, errors.Cause(err))

			ctrl.Finish()
		})
	}
}

func TestCreateOrUpdateDeployment_PreservesForeignMetadata(t *testing.T) {
	tests := []struct {
		name                        string
		liveDeployment              *appsv1.Deployment
		desiredDeployment           *appsv1.Deployment
		expectedAnnotations         map[string]string // Deployment metadata annotations
		expectedLabels              map[string]string // Deployment metadata labels
		expectedTemplateAnnotations map[string]string // Pod template annotations
		expectedTemplateLabels      map[string]string // Pod template labels
	}{
		{
			name: "preserves kubectl.kubernetes.io/restartedAt annotation on pod template",
			liveDeployment: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-deployment",
					Namespace: "test-namespace",
				},
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{
								"kubectl.kubernetes.io/restartedAt": "2024-11-10T13:25:00Z",
							},
						},
					},
				},
			},
			desiredDeployment: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-deployment",
					Namespace: "test-namespace",
				},
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{},
						},
					},
				},
			},
			expectedTemplateAnnotations: map[string]string{
				"kubectl.kubernetes.io/restartedAt": "2024-11-10T13:25:00Z",
			},
		},
		{
			name: "CSV annotations win over live annotations",
			liveDeployment: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-deployment",
					Namespace: "test-namespace",
					Annotations: map[string]string{
						"olm.targetNamespaces": "old-namespace",
						"user-annotation":      "user-value",
					},
				},
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{
								"operatorframework.io/properties":   "old-props",
								"kubectl.kubernetes.io/restartedAt": "2024-11-10T13:25:00Z",
							},
						},
					},
				},
			},
			desiredDeployment: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-deployment",
					Namespace: "test-namespace",
					Annotations: map[string]string{
						"olm.targetNamespaces": "new-namespace",
					},
				},
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{
								"operatorframework.io/properties": "new-props",
							},
						},
					},
				},
			},
			expectedAnnotations: map[string]string{
				"olm.targetNamespaces": "new-namespace", // CSV wins
				"user-annotation":      "user-value",    // preserved
			},
			expectedTemplateAnnotations: map[string]string{
				"operatorframework.io/properties":   "new-props",             // CSV wins
				"kubectl.kubernetes.io/restartedAt": "2024-11-10T13:25:00Z", // preserved
			},
		},
		{
			name: "preserves foreign labels on pod template",
			liveDeployment: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-deployment",
					Namespace: "test-namespace",
					Labels: map[string]string{
						"user-label": "user-value",
						"olm.owner":  "old-csv",
					},
				},
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app":            "my-app",
								"olm.managed":    "true",
								"user-pod-label": "user-pod-value",
							},
						},
					},
				},
			},
			desiredDeployment: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-deployment",
					Namespace: "test-namespace",
					Labels: map[string]string{
						"olm.owner": "new-csv",
					},
				},
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app":         "my-app",
								"olm.managed": "true",
							},
						},
					},
				},
			},
			expectedLabels: map[string]string{
				"olm.owner":  "new-csv",    // CSV wins
				"user-label": "user-value", // preserved
			},
			expectedTemplateLabels: map[string]string{
				"app":            "my-app",
				"olm.managed":    "true",
				"user-pod-label": "user-pod-value", // preserved
			},
		},
		{
			name: "does not preserve system annotations",
			liveDeployment: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-deployment",
					Namespace: "test-namespace",
					Annotations: map[string]string{
						"kubectl.kubernetes.io/last-applied-configuration": "{}",
						"deployment.kubernetes.io/revision":                 "5",
						"user-annotation":                                   "user-value",
					},
				},
			},
			desiredDeployment: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-deployment",
					Namespace: "test-namespace",
				},
			},
			expectedAnnotations: map[string]string{
				"user-annotation": "user-value", // preserved
				// system annotations NOT preserved
			},
		},
		{
			name: "handles nil annotations and labels gracefully",
			liveDeployment: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-deployment",
					Namespace: "test-namespace",
					Annotations: map[string]string{
						"user-annotation": "user-value",
					},
				},
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{
								"kubectl.kubernetes.io/restartedAt": "2024-11-10T13:25:00Z",
							},
						},
					},
				},
			},
			desiredDeployment: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-deployment",
					Namespace: "test-namespace",
					// Annotations is nil
				},
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							// Annotations is nil
						},
					},
				},
			},
			expectedAnnotations: map[string]string{
				"user-annotation": "user-value",
			},
			expectedTemplateAnnotations: map[string]string{
				"kubectl.kubernetes.io/restartedAt": "2024-11-10T13:25:00Z",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			// Create a fake Kubernetes client with the live deployment
			fakeClient := fake.NewSimpleClientset(tt.liveDeployment)

			// Create mock operatorclient that wraps the fake client
			mockOpClient := operatorclientmocks.NewMockClientInterface(ctrl)
			fakeLister := &operatorlisterfakes.FakeOperatorLister{}

			client := NewInstallStrategyDeploymentClient(mockOpClient, fakeLister, "test-namespace")

			// Setup mock to return live deployment on GET
			mockOpClient.EXPECT().
				GetDeployment(tt.liveDeployment.Namespace, tt.liveDeployment.Name).
				Return(tt.liveDeployment, nil).
				Times(1)

			// Return the fake kubernetes client
			mockOpClient.EXPECT().
				KubernetesInterface().
				Return(fakeClient).
				Times(1)

			// Call the function
			_, err := client.CreateOrUpdateDeployment(tt.desiredDeployment)
			require.NoError(t, err)

			// Retrieve the updated deployment from the fake client
			updatedDeployment, err := fakeClient.AppsV1().Deployments(tt.desiredDeployment.Namespace).Get(
				context.TODO(), tt.desiredDeployment.Name, metav1.GetOptions{})
			require.NoError(t, err)

			// Verify annotations on Deployment metadata
			if tt.expectedAnnotations != nil {
				require.Equal(t, tt.expectedAnnotations, updatedDeployment.Annotations,
					"Deployment annotations mismatch")
			}

			// Verify labels on Deployment metadata
			if tt.expectedLabels != nil {
				require.Equal(t, tt.expectedLabels, updatedDeployment.Labels,
					"Deployment labels mismatch")
			}

			// Verify annotations on Pod Template
			if tt.expectedTemplateAnnotations != nil {
				require.Equal(t, tt.expectedTemplateAnnotations, updatedDeployment.Spec.Template.Annotations,
					"Pod template annotations mismatch")
			}

			// Verify labels on Pod Template
			if tt.expectedTemplateLabels != nil {
				require.Equal(t, tt.expectedTemplateLabels, updatedDeployment.Spec.Template.Labels,
					"Pod template labels mismatch")
			}
		})
	}
}

func TestCreateOrUpdateDeployment_CreatesWhenNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockOpClient := operatorclientmocks.NewMockClientInterface(ctrl)
	fakeLister := &operatorlisterfakes.FakeOperatorLister{}

	client := NewInstallStrategyDeploymentClient(mockOpClient, fakeLister, "test-namespace")

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-deployment",
			Namespace: "test-namespace",
			Annotations: map[string]string{
				"csv-annotation": "csv-value",
			},
		},
	}

	// Expect GET to return NotFound
	mockOpClient.EXPECT().
		GetDeployment(deployment.Namespace, deployment.Name).
		Return(nil, apierrors.NewNotFound(appsv1.Resource("deployments"), deployment.Name)).
		Times(1)

	// Expect CREATE to be called (no merge should happen)
	mockOpClient.EXPECT().
		CreateDeployment(deployment).
		Return(deployment, nil).
		Times(1)

	result, err := client.CreateOrUpdateDeployment(deployment)
	require.NoError(t, err)
	require.Equal(t, deployment, result)
}
