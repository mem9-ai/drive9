---
title: Alicloud Terraform root
---

This root is a public-safe Alicloud ACK Terraform template for a drive9 server
environment. Account-specific values must be supplied through an ignored local
`terraform.tfvars` file.

Do not run `terraform apply` until the first plan has been reviewed and the
account-specific values are confirmed.

## Scope

Managed here:

1. VPC, worker VSwitches, Terway pod VSwitches, and ACK security group.
2. ACK managed Kubernetes cluster and one node pool.
3. KMS key for Kubernetes secret encryption and drive9 object encryption.
4. OSS bucket with private ACL, versioning, and SSE-KMS.
5. KMS secret policy reference for the drive9 server bootstrap secret.
6. RRSA RAM roles and policies for the drive9 server service account and
   External Secrets service account.
7. ACK ACR credential helper add-on for private image pulls.

Not managed here yet:

1. Kubernetes resources such as namespaces, service accounts, deployments,
   ingresses, ExternalSecret objects, Helm releases, and RBAC.
2. Alicloud ALB/WAF resources created or owned by ingress controllers.
3. TiDB, db9, observability, DNS, and certificate resources.
4. Kubernetes manifests that annotate service accounts or configure External
   Secrets `SecretStore` RRSA fields.

## Local workflow

```bash
cd tf/ali-dev-ap-southeast-1
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with the real private environment values.
terraform init
terraform plan
```

## Required inputs before first plan

1. Copy `terraform.tfvars.example` to `terraform.tfvars`.
2. Set the real Alicloud account ID, profile, region, ACR instance ID, optional
   OSS bucket name override, KMS secret name, KMS encryption key ID, CIDRs, namespace, and
   service account values.
3. Create and populate the KMS secret outside Terraform. Do not manage real
   credentials in Terraform state.

The committed files intentionally do not contain real account IDs, cluster IDs,
ACR instance IDs, KMS key IDs, secret names, or environment CIDRs.

## State

No remote backend is configured. `terraform.tfstate` files are local and must
not be committed.
