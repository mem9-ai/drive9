locals {
  acr_repository_arn        = "acs:cr:${var.alicloud_region}:${var.alicloud_account_id}:repository/${var.acr_instance_id}/${var.acr_repository_namespace}/${local.acr_repository_name}"
  github_deploy_policy_name = "${local.name_prefix}-github-deploy"
  github_deploy_role_name   = "${local.cluster_name}-github-deploy"
  github_oidc_issuer_url    = "https://token.actions.githubusercontent.com"
}

resource "alicloud_ims_oidc_provider" "github_actions" {
  client_ids          = ["sts.aliyuncs.com"]
  description         = "GitHub Actions OIDC provider"
  fingerprints        = var.github_oidc_fingerprints
  issuance_limit_time = 12
  issuer_url          = local.github_oidc_issuer_url
  oidc_provider_name  = var.github_oidc_provider_name
}

resource "alicloud_ram_role" "github_deploy" {
  assume_role_policy_document = jsonencode({
    Version = "1"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "oidc:aud" = "sts.aliyuncs.com"
            "oidc:iss" = local.github_oidc_issuer_url
            "oidc:sub" = var.github_deploy_subjects
          }
        }
        Effect = "Allow"
        Principal = {
          Federated = [
            alicloud_ims_oidc_provider.github_actions.arn,
          ]
        }
      },
    ]
  })
  description          = "GitHub Actions OIDC deploy role for ${local.cluster_name}"
  max_session_duration = 3600
  role_name            = local.github_deploy_role_name
  tags                 = local.common_tags
}

resource "alicloud_ram_policy" "github_deploy" {
  description = "Allow GitHub Actions to push ${local.acr_repository_name} and read ${local.cluster_name} kubeconfig"
  policy_document = jsonencode({
    Version = "1"
    Statement = [
      {
        Action = [
          "cr:GetAuthorizationToken",
        ]
        Effect   = "Allow"
        Resource = ["*"]
      },
      {
        Action = [
          "cr:PullRepository",
          "cr:PushRepository",
        ]
        Effect = "Allow"
        Resource = [
          local.acr_repository_arn,
        ]
      },
      {
        Action = [
          "cs:DescribeClusterUserKubeconfig",
        ]
        Effect = "Allow"
        Resource = [
          "acs:cs:${var.alicloud_region}:${var.alicloud_account_id}:cluster/${alicloud_cs_managed_kubernetes.main.id}",
        ]
      },
    ]
  })
  policy_name = local.github_deploy_policy_name
}

resource "alicloud_ram_role_policy_attachment" "github_deploy" {
  policy_name = alicloud_ram_policy.github_deploy.policy_name
  policy_type = alicloud_ram_policy.github_deploy.type
  role_name   = alicloud_ram_role.github_deploy.role_name
}
