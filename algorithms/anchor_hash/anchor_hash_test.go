package anchor_hash

import "testing"

func TestAnchor(t *testing.T) {
	bucket := 8
	used := 6
	anchor := NewAnchorHash(bucket, used)
	anchor.Print()

	anchor.RemoveBucket(2)
	anchor.Print()

	anchor.AddBucket()
	anchor.Print()

}
