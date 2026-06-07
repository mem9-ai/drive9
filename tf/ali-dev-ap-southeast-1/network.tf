data "alicloud_enhanced_nat_available_zones" "available" {}

resource "alicloud_vpc" "main" {
  cidr_block = var.vpc_cidr
  tags       = local.common_tags
  vpc_name   = local.vpc_name
}

resource "alicloud_vswitch" "worker" {
  for_each = var.worker_vswitches

  cidr_block   = each.value.cidr_block
  tags         = local.common_tags
  vpc_id       = alicloud_vpc.main.id
  vswitch_name = "${local.name_prefix}-vswitch-worker-${data.alicloud_enhanced_nat_available_zones.available.zones[each.value.zone_index].zone_id}"
  zone_id      = data.alicloud_enhanced_nat_available_zones.available.zones[each.value.zone_index].zone_id
}

resource "alicloud_vswitch" "pod" {
  for_each = var.pod_vswitches

  cidr_block   = each.value.cidr_block
  tags         = local.common_tags
  vpc_id       = alicloud_vpc.main.id
  vswitch_name = "${local.name_prefix}-vswitch-pod-${data.alicloud_enhanced_nat_available_zones.available.zones[each.value.zone_index].zone_id}"
  zone_id      = data.alicloud_enhanced_nat_available_zones.available.zones[each.value.zone_index].zone_id
}

resource "alicloud_security_group" "cluster" {
  description         = "ACK security group for ${local.name_prefix}"
  inner_access_policy = "Accept"
  security_group_name = local.security_group_name
  security_group_type = "normal"
  tags                = local.common_tags
  vpc_id              = alicloud_vpc.main.id
}

resource "alicloud_security_group_rule" "vpc_ingress" {
  cidr_ip           = var.vpc_cidr
  ip_protocol       = "all"
  nic_type          = "intranet"
  policy            = "accept"
  port_range        = "-1/-1"
  priority          = 1
  security_group_id = alicloud_security_group.cluster.id
  type              = "ingress"
}

resource "alicloud_security_group_rule" "egress" {
  cidr_ip           = "0.0.0.0/0"
  ip_protocol       = "all"
  nic_type          = "intranet"
  policy            = "accept"
  port_range        = "-1/-1"
  priority          = 1
  security_group_id = alicloud_security_group.cluster.id
  type              = "egress"
}
