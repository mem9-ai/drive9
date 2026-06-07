locals {
  external_secrets_policy_name    = "${local.name_prefix}-external-secrets-kms"
  external_secrets_rrsa_role_name = "${local.name_prefix}-external-secrets-rrsa"
  server_policy_name              = "${local.name_prefix}-server-storage"
  server_rrsa_role_name           = "${local.name_prefix}-server-rrsa"

  external_secrets_service_account_subject = "system:serviceaccount:${var.external_secrets_namespace}:${var.external_secrets_service_account_name}"
  server_service_account_subject           = "system:serviceaccount:${var.server_namespace}:${var.server_service_account_name}"

  oss_bucket_arn        = "acs:oss:*:${var.alicloud_account_id}:${local.oss_bucket_name}"
  oss_objects_arn       = "${local.oss_bucket_arn}/*"
  server_secret_arn     = "acs:kms:${var.alicloud_region}:${var.alicloud_account_id}:secret/${var.server_secret_name}"
  server_secret_key_arn = "acs:kms:${var.alicloud_region}:${var.alicloud_account_id}:key/${var.server_secret_encryption_key_id}"

  rrsa_oidc_issuer_url   = alicloud_cs_managed_kubernetes.main.rrsa_metadata[0].rrsa_oidc_issuer_url
  rrsa_oidc_provider_arn = alicloud_cs_managed_kubernetes.main.rrsa_metadata[0].ram_oidc_provider_arn
}

resource "alicloud_ram_role" "external_secrets" {
  assume_role_policy_document = jsonencode({
    Version = "1"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "oidc:aud" = "sts.aliyuncs.com"
            "oidc:iss" = local.rrsa_oidc_issuer_url
            "oidc:sub" = local.external_secrets_service_account_subject
          }
        }
        Effect = "Allow"
        Principal = {
          Federated = [
            local.rrsa_oidc_provider_arn,
          ]
        }
      },
    ]
  })
  description          = "External Secrets RRSA role for ${local.name_prefix}"
  max_session_duration = 3600
  role_name            = local.external_secrets_rrsa_role_name
  tags                 = local.common_tags
}

resource "alicloud_ram_policy" "external_secrets" {
  description = "Allow External Secrets to read ${local.name_prefix} KMS secrets"
  policy_document = jsonencode({
    Version = "1"
    Statement = [
      {
        Action = [
          "kms:DescribeSecret",
          "kms:GetSecretValue",
        ]
        Effect = "Allow"
        Resource = [
          local.server_secret_arn,
        ]
      },
      {
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey",
        ]
        Effect = "Allow"
        Resource = [
          local.server_secret_key_arn,
        ]
      },
    ]
  })
  policy_name = local.external_secrets_policy_name
}

resource "alicloud_ram_role_policy_attachment" "external_secrets" {
  policy_name = alicloud_ram_policy.external_secrets.policy_name
  policy_type = alicloud_ram_policy.external_secrets.type
  role_name   = alicloud_ram_role.external_secrets.role_name
}

resource "alicloud_ram_role" "server" {
  assume_role_policy_document = jsonencode({
    Version = "1"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "oidc:aud" = "sts.aliyuncs.com"
            "oidc:iss" = local.rrsa_oidc_issuer_url
            "oidc:sub" = local.server_service_account_subject
          }
        }
        Effect = "Allow"
        Principal = {
          Federated = [
            local.rrsa_oidc_provider_arn,
          ]
        }
      },
    ]
  })
  description          = "${local.server_display_name} server RRSA role for ${local.name_prefix}"
  max_session_duration = 3600
  role_name            = local.server_rrsa_role_name
  tags                 = local.common_tags
}

resource "alicloud_ram_policy" "server" {
  description = "Allow ${local.server_display_name} server to access OSS and KMS for ${local.name_prefix}"
  policy_document = jsonencode({
    Version = "1"
    Statement = [
      {
        Action = [
          "oss:ListObjects",
          "oss:ListMultipartUploads",
        ]
        Effect = "Allow"
        Resource = [
          local.oss_bucket_arn,
        ]
      },
      {
        Action = [
          "oss:AbortMultipartUpload",
          "oss:DeleteObject",
          "oss:GetObject",
          "oss:ListParts",
          "oss:PutObject",
        ]
        Effect = "Allow"
        Resource = [
          local.oss_objects_arn,
        ]
      },
      {
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey",
          "kms:Encrypt",
          "kms:GenerateDataKey",
        ]
        Effect = "Allow"
        Resource = [
          alicloud_kms_key.server.arn,
        ]
      },
    ]
  })
  policy_name = local.server_policy_name
}

resource "alicloud_ram_role_policy_attachment" "server" {
  policy_name = alicloud_ram_policy.server.policy_name
  policy_type = alicloud_ram_policy.server.type
  role_name   = alicloud_ram_role.server.role_name
}
