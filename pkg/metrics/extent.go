package metrics

var extentPutBytes = serviceMeter.Int64Counter("drive9_extent_put_bytes_total", "S3 PUT Content-Length sum for content_layout=extent blocks")

// RecordExtentPutBytes records immutable-block PUT bytes (including 412 retries counted at original size).
func RecordExtentPutBytes(n int64) {
	if n <= 0 {
		return
	}
	extentPutBytes.Add(n)
}
