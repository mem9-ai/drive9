data "alicloud_instance_types" "worker" {
  availability_zone    = data.alicloud_enhanced_nat_available_zones.available.zones[0].zone_id
  cpu_core_count       = 2
  kubernetes_node_role = "Worker"
  memory_size          = 4
  system_disk_category = var.node_system_disk_category
  instance_charge_type = "PostPaid"
  sorted_by            = "Price"
}

resource "alicloud_cs_managed_kubernetes" "main" {
  cluster_spec                   = var.cluster_spec
  deletion_protection            = var.cluster_deletion_protection
  disable_encryption             = true
  enable_rrsa                    = true
  name                           = local.cluster_name
  new_nat_gateway                = true
  pod_vswitch_ids                = [for vswitch in alicloud_vswitch.pod : vswitch.id]
  proxy_mode                     = "ipvs"
  security_group_id              = alicloud_security_group.cluster.id
  service_cidr                   = var.service_cidr
  skip_set_certificate_authority = true
  tags                           = local.common_tags
  version                        = var.cluster_version
  vswitch_ids                    = [for vswitch in alicloud_vswitch.worker : vswitch.id]

  audit_log_config {
    enabled = false
  }

  addons {
    name = "terway-eniip"
  }

  addons {
    name = "csi-plugin"
  }

  addons {
    name = "csi-provisioner"
  }

  addons {
    name = "ack-pod-identity-webhook"
  }
}

resource "alicloud_cs_kubernetes_addon" "ack_ram_authenticator" {
  cluster_id = alicloud_cs_managed_kubernetes.main.id
  name       = "ack-ram-authenticator"
  version    = "0.5.1"

  config = jsonencode({
    EnableNonBootstrapMapping = true
  })
}

resource "alicloud_cs_kubernetes_addon" "alb_ingress_controller" {
  cluster_id = alicloud_cs_managed_kubernetes.main.id
  name       = "alb-ingress-controller"
  version    = "v2.20.0"
}

resource "alicloud_cs_kubernetes_addon" "acr_credential_helper" {
  cluster_id = alicloud_cs_managed_kubernetes.main.id
  name       = "managed-aliyun-acr-credential-helper"
  version    = var.acr_credential_helper_version

  config = jsonencode({
    AcrInstanceInfo = [
      {
        instanceId = var.acr_instance_id
        regionId   = var.alicloud_region
      },
    ]

    enableRRSA        = false
    expiringThreshold = var.acr_credential_helper_expiring_threshold
    serviceAccount    = var.server_service_account_name
    watchNamespace    = var.server_namespace
  })
}

resource "alicloud_cs_kubernetes_node_pool" "main" {
  cluster_id           = alicloud_cs_managed_kubernetes.main.id
  desired_size         = var.node_desired_size
  instance_charge_type = "PostPaid"
  instance_types       = local.node_instance_types
  node_pool_name       = local.node_pool_name
  system_disk_category = var.node_system_disk_category
  system_disk_size     = var.node_system_disk_size
  vswitch_ids          = [for vswitch in alicloud_vswitch.worker : vswitch.id]
}
