terraform {
  required_version = ">= 1.5.0, < 2.0.0"

  required_providers {
    alicloud = {
      source  = "aliyun/alicloud"
      version = ">= 1.279.0, < 2.0.0"
    }
  }
}

provider "alicloud" {
  profile                 = var.alicloud_profile
  region                  = var.alicloud_region
  shared_credentials_file = var.alicloud_shared_credentials_file

  sign_version {
    oss = "v4"
  }
}
