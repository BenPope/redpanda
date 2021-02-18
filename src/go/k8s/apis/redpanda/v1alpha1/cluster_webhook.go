// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package v1alpha1 represent Custom Resource definition of the vectorized.io redpanda group
package v1alpha1

import (
	"reflect"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

// log is for logging in this package.
var log = logf.Log.WithName("cluster-resource")

// SetupWebhookWithManager autogenerated function by kubebuilder
func (r *Cluster) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(r).
		Complete()
}

//+kubebuilder:webhook:path=/mutate-redpanda-vectorized-io-v1alpha1-cluster,mutating=true,failurePolicy=fail,sideEffects=None,groups=redpanda.vectorized.io,resources=clusters,verbs=create;update,versions=v1alpha1,name=mcluster.kb.io,admissionReviewVersions={v1,v1beta1}

var _ webhook.Defaulter = &Cluster{}

// Default implements webhook.Defaulter so a webhook will be registered for the type
// TODO(user): fill in your defaulting logic.
func (r *Cluster) Default() {
	log.Info("default", "name", r.Name)
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
//+kubebuilder:webhook:path=/validate-redpanda-vectorized-io-v1alpha1-cluster,mutating=false,failurePolicy=fail,sideEffects=None,groups=redpanda.vectorized.io,resources=clusters,verbs=create;update,versions=v1alpha1,name=vcluster.kb.io,admissionReviewVersions={v1,v1beta1}

var _ webhook.Validator = &Cluster{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (r *Cluster) ValidateCreate() error {
	log.Info("validate create", "name", r.Name)

	var allErrs field.ErrorList

	if err := r.checkCollidingPorts(); err != nil {
		allErrs = append(allErrs, err...)
	}

	if len(allErrs) == 0 {
		return nil
	}

	return apierrors.NewInvalid(
		r.GroupVersionKind().GroupKind(),
		r.Name, allErrs)
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (r *Cluster) ValidateUpdate(old runtime.Object) error {
	log.Info("validate update", "name", r.Name)
	oldCluster := old.(*Cluster)
	var allErrs field.ErrorList

	if r.Spec.Replicas != nil && oldCluster.Spec.Replicas != nil && *r.Spec.Replicas < *oldCluster.Spec.Replicas {
		allErrs = append(allErrs,
			field.Invalid(field.NewPath("spec").Child("replicas"),
				r.Spec.Replicas,
				"scaling down is not supported"))
	}
	if !reflect.DeepEqual(r.Spec.Configuration, oldCluster.Spec.Configuration) {
		allErrs = append(allErrs,
			field.Invalid(field.NewPath("spec").Child("configuration"),
				r.Spec.Configuration,
				"updating configuration is not supported"))
	}

	if err := r.checkCollidingPorts(); err != nil {
		allErrs = append(allErrs, err...)
	}

	if len(allErrs) == 0 {
		return nil
	}

	return apierrors.NewInvalid(
		r.GroupVersionKind().GroupKind(),
		r.Name, allErrs)
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type
func (r *Cluster) ValidateDelete() error {
	log.Info("validate delete", "name", r.Name)

	// TODO(user): fill in your validation logic upon object deletion.
	return nil
}

func (r *Cluster) checkCollidingPorts() field.ErrorList {
	var allErrs field.ErrorList
	if r.Spec.ExternalConnectivity {
		if r.Spec.Configuration.KafkaAPI.Port+1 == r.Spec.Configuration.AdminAPI.Port {
			allErrs = append(allErrs,
				field.Invalid(field.NewPath("spec").Child("configuration", "admin", "port"),
					r.Spec.Configuration.AdminAPI.Port,
					"admin port will collide with external kafka api port when ExternalConnectivity is turn on"))
		}
		if r.Spec.Configuration.KafkaAPI.Port+1 == r.Spec.Configuration.RPCServer.Port {
			allErrs = append(allErrs,
				field.Invalid(field.NewPath("spec").Child("configuration", "rpcServer", "port"),
					r.Spec.Configuration.RPCServer.Port,
					"rpcServer port will collide with external kafka api port when ExternalConnectivity is turn on"))
		}
	}

	if r.Spec.Configuration.AdminAPI.Port == r.Spec.Configuration.KafkaAPI.Port {
		allErrs = append(allErrs,
			field.Invalid(field.NewPath("spec").Child("configuration", "admin", "port"),
				r.Spec.Configuration.AdminAPI.Port,
				"admin port collide with Spec.Configuration.KafkaAPI.Port"))
	}

	if r.Spec.Configuration.RPCServer.Port == r.Spec.Configuration.KafkaAPI.Port {
		allErrs = append(allErrs,
			field.Invalid(field.NewPath("spec").Child("configuration", "rpcServer", "port"),
				r.Spec.Configuration.RPCServer.Port,
				"rpc port collide with Spec.Configuration.KafkaAPI.Port"))
	}

	if r.Spec.Configuration.AdminAPI.Port == r.Spec.Configuration.RPCServer.Port {
		allErrs = append(allErrs,
			field.Invalid(field.NewPath("spec").Child("configuration", "admin", "port"),
				r.Spec.Configuration.AdminAPI.Port,
				"admin port collide with Spec.Configuration.RPCServer.Port"))
	}

	return allErrs
}
