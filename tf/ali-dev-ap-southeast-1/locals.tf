locals {
  account_suffix = substr(var.alicloud_account_id, length(var.alicloud_account_id) - 4, 4)
  name_prefix    = "${var.environment}-${var.project}"

  common_tags = {
    cloud       = "alicloud"
    component   = var.component
    env         = var.environment
    environment = var.environment
    project     = var.project
  }

  node_instance_types = length(var.node_instance_types) > 0 ? var.node_instance_types : [
    data.alicloud_instance_types.worker.instance_types[0].id,
  ]

  acr_repository_name = coalesce(var.acr_repository_name, var.component)

  cluster_name        = "${local.name_prefix}-ack-${var.alicloud_region}"
  node_pool_name      = "${local.name_prefix}-nodepool-private"
  oss_bucket_name     = coalesce(var.oss_bucket_name, "${local.name_prefix}-oss-${var.alicloud_region}-${local.account_suffix}")
  security_group_name = "${local.name_prefix}-ack-cluster-sg-${var.alicloud_region}"
  server_display_name = replace(var.component, "-server", "")
  vpc_name            = "${local.name_prefix}-vpc-${var.alicloud_region}"
}
