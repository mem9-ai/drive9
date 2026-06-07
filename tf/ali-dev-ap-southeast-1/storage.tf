resource "alicloud_kms_key" "server" {
  automatic_rotation     = "Disabled"
  description            = "KMS key for ${local.name_prefix} drive9 server encryption"
  key_spec               = "Aliyun_AES_256"
  key_usage              = "ENCRYPT/DECRYPT"
  origin                 = "Aliyun_KMS"
  pending_window_in_days = 7
  protection_level       = "SOFTWARE"
  status                 = "Enabled"
  tags                   = local.common_tags
}

resource "alicloud_oss_bucket" "server" {
  bucket          = local.oss_bucket_name
  force_destroy   = false
  redundancy_type = var.oss_redundancy_type
  storage_class   = "Standard"
  tags            = local.common_tags

  lifecycle {
    ignore_changes = [
      server_side_encryption_rule,
      versioning,
    ]
  }
}

resource "alicloud_oss_bucket_acl" "server" {
  acl    = "private"
  bucket = alicloud_oss_bucket.server.bucket
}

resource "alicloud_oss_bucket_versioning" "server" {
  bucket = alicloud_oss_bucket.server.bucket
  status = "Enabled"
}

resource "alicloud_oss_bucket_server_side_encryption" "server" {
  bucket            = alicloud_oss_bucket.server.bucket
  kms_master_key_id = alicloud_kms_key.server.id
  sse_algorithm     = "KMS"
}
