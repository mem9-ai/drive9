output "cluster_id" {
  description = "ACK cluster ID."
  value       = alicloud_cs_managed_kubernetes.main.id
}

output "cluster_name" {
  description = "ACK cluster name."
  value       = alicloud_cs_managed_kubernetes.main.name
}

output "external_secrets_rrsa_role_arn" {
  description = "RAM role ARN trusted by the External Secrets Kubernetes service account."
  value       = alicloud_ram_role.external_secrets.arn
}

output "external_secrets_service_account_subject" {
  description = "OIDC subject trusted by the External Secrets RRSA role."
  value       = local.external_secrets_service_account_subject
}

output "github_deploy_kubernetes_group" {
  description = "Kubernetes group to bind for GitHub Actions deployment RBAC."
  value       = var.github_deploy_kubernetes_group
}

output "github_deploy_role_arn" {
  description = "RAM role ARN trusted by GitHub Actions for this ACK deployment."
  value       = alicloud_ram_role.github_deploy.arn
}

output "github_oidc_provider_arn" {
  description = "IMS OIDC provider ARN used by GitHub Actions."
  value       = alicloud_ims_oidc_provider.github_actions.arn
}

output "github_server_image_repository" {
  description = "ACR image repository used by the drive9 server GitHub deployment."
  value       = "${var.alicloud_account_id}-registry.${var.alicloud_region}.cr.aliyuncs.com/${var.acr_repository_namespace}/${local.acr_repository_name}"
}

output "kms_key_id" {
  description = "KMS key ID used for OSS SSE-KMS."
  value       = alicloud_kms_key.server.id
}

output "oss_bucket_name" {
  description = "OSS bucket name used by the drive9 server."
  value       = alicloud_oss_bucket.server.bucket
}

output "pod_vswitch_ids" {
  description = "Terway pod VSwitch IDs."
  value       = { for key, vswitch in alicloud_vswitch.pod : key => vswitch.id }
}

output "rrsa_metadata" {
  description = "ACK RRSA metadata used when creating RAM roles for service accounts."
  value       = alicloud_cs_managed_kubernetes.main.rrsa_metadata
}

output "rrsa_oidc_provider_arn" {
  description = "ACK RRSA OIDC provider ARN."
  value       = local.rrsa_oidc_provider_arn
}

output "security_group_id" {
  description = "ACK security group ID."
  value       = alicloud_security_group.cluster.id
}

output "server_rrsa_role_arn" {
  description = "RAM role ARN trusted by the dat9 server Kubernetes service account."
  value       = alicloud_ram_role.server.arn
}

output "server_secret_name" {
  description = "Expected KMS secret name for the drive9 server bootstrap secret."
  value       = var.server_secret_name
}

output "server_secret_arn" {
  description = "Expected KMS secret ARN for the drive9 server bootstrap secret."
  value       = local.server_secret_arn
}

output "server_service_account_subject" {
  description = "OIDC subject trusted by the dat9 server RRSA role."
  value       = local.server_service_account_subject
}

output "vpc_id" {
  description = "VPC ID."
  value       = alicloud_vpc.main.id
}

output "worker_vswitch_ids" {
  description = "ACK worker VSwitch IDs."
  value       = { for key, vswitch in alicloud_vswitch.worker : key => vswitch.id }
}
