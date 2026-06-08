package s3client

import "os"

// aliyunCredentials returns the Alibaba Cloud access credentials from the
// standard ALIBABA_CLOUD_* environment variables that ACK injects via RRSA or
// ECS RAM roles. Returns empty strings when the variables are not set.
func aliyunCredentials() (accessKeyID, secretAccessKey, securityToken string) {
	return os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_ID"),
		os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET"),
		os.Getenv("ALIBABA_CLOUD_SECURITY_TOKEN")
}

// isAliyunEndpoint reports whether endpoint points to an Aliyun OSS service.
func isAliyunEndpoint(endpoint string) bool {
	return len(endpoint) > 0 && containsAliyunDomain(endpoint)
}

func containsAliyunDomain(s string) bool {
	// Cover oss-*.aliyuncs.com and kms*.aliyuncs.com variants.
	for i := 0; i+len("aliyuncs.com") <= len(s); i++ {
		if s[i:i+len("aliyuncs.com")] == "aliyuncs.com" {
			return true
		}
	}
	return false
}
