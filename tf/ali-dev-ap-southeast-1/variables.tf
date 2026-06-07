variable "acr_credential_helper_expiring_threshold" {
  description = "How early before ACR pull credentials expire the helper refreshes them."
  type        = string
  default     = "15m"
}

variable "acr_credential_helper_version" {
  description = "ACK managed ACR credential helper add-on version."
  type        = string
  default     = "v24.01.29.1-5318af4-aliyun"
}

variable "acr_instance_id" {
  description = "ACR Enterprise Edition instance ID used for private application images."
  type        = string
}

variable "acr_repository_name" {
  description = "ACR repository name used for the drive9 server image. When null, component is used."
  type        = string
  default     = null
}

variable "acr_repository_namespace" {
  description = "ACR namespace used for the drive9 server image repository."
  type        = string
  default     = "drive9"
}

variable "alicloud_account_id" {
  description = "Alibaba Cloud account ID for resource-scoped RAM policy ARNs."
  type        = string
}

variable "alicloud_profile" {
  description = "Alibaba Cloud profile from the shared credentials file."
  type        = string
}

variable "alicloud_region" {
  description = "Alibaba Cloud region for this environment."
  type        = string
}

variable "alicloud_shared_credentials_file" {
  description = "Optional Alibaba Cloud shared credentials file path."
  type        = string
  default     = null
}

variable "cluster_deletion_protection" {
  description = "Whether ACK deletion protection is enabled."
  type        = bool
  default     = false
}

variable "cluster_spec" {
  description = "ACK cluster specification."
  type        = string
  default     = "ack.pro.small"
}

variable "cluster_version" {
  description = "ACK Kubernetes version. Leave null to let ACK choose the latest supported version at creation time."
  type        = string
  default     = null
}

variable "component" {
  description = "Component tag value."
  type        = string
  default     = "drive9-server"
}

variable "environment" {
  description = "Environment tag value."
  type        = string
  default     = "dev"
}

variable "github_deploy_kubernetes_group" {
  description = "Kubernetes RBAC group used by the GitHub deploy RAM identity mapping."
  type        = string
  default     = "dat9-github-deploy"
}

variable "github_deploy_subjects" {
  description = "GitHub Actions OIDC subjects allowed to assume the per-ACK deploy role."
  type        = list(string)
  default     = ["repo:mem9-ai/drive9:ref:refs/heads/main"]
}

variable "github_oidc_fingerprints" {
  description = "SHA1 CA certificate fingerprints trusted for the GitHub Actions OIDC provider."
  type        = list(string)
  default = [
    "22FF89586561FC2D52F77491E9F1EFF1B80BE33E",
    "CABD2A79A1076A31F21D253635CB039D4329A5E8",
  ]
}

variable "github_oidc_provider_name" {
  description = "Alibaba Cloud IMS OIDC provider name for GitHub Actions."
  type        = string
  default     = "github-actions"
}

variable "external_secrets_namespace" {
  description = "Kubernetes namespace used by the External Secrets service account."
  type        = string
  default     = "external-secrets"
}

variable "external_secrets_service_account_name" {
  description = "Kubernetes service account name used by External Secrets."
  type        = string
  default     = "external-secrets"
}

variable "node_desired_size" {
  description = "Desired ACK node pool size."
  type        = number
  default     = 3
}

variable "node_instance_types" {
  description = "ACK node instance types."
  type        = list(string)
  default     = ["ecs.g7.xlarge"]
}

variable "node_system_disk_category" {
  description = "ACK node system disk category."
  type        = string
  default     = "cloud_essd"
}

variable "node_system_disk_size" {
  description = "ACK node system disk size in GiB."
  type        = number
  default     = 80
}

variable "oss_bucket_name" {
  description = "Optional OSS bucket name override. When null, the name is derived from environment, project, region, and account suffix because OSS bucket names are globally unique."
  type        = string
  default     = null
}

variable "oss_redundancy_type" {
  description = "OSS bucket redundancy type."
  type        = string
  default     = "LRS"
}

variable "project" {
  description = "Project tag value."
  type        = string
  default     = "drive9"
}

variable "service_cidr" {
  description = "Kubernetes service CIDR. It must not overlap VPC, VSwitch, or pod CIDRs."
  type        = string
}

variable "server_secret_name" {
  description = "KMS secret name for the drive9 server bootstrap secret."
  type        = string
}

variable "server_secret_encryption_key_id" {
  description = "Existing dedicated KMS key ID used to encrypt the external drive9 server secret."
  type        = string
}

variable "server_namespace" {
  description = "Kubernetes namespace used by the drive9 server service account."
  type        = string
}

variable "server_service_account_name" {
  description = "Kubernetes service account name used by the drive9 server."
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR block for the Alicloud VPC."
  type        = string
}

variable "worker_vswitches" {
  description = "Worker node VSwitches keyed by short zone alias."
  type = map(object({
    cidr_block = string
    zone_index = number
  }))
}

variable "pod_vswitches" {
  description = "Terway pod VSwitches keyed by short zone alias."
  type = map(object({
    cidr_block = string
    zone_index = number
  }))
}
