package anchor_hash

import (
	"consistent-hash/utils"
	"fmt"
)

type AnchorHash struct {
	A []uint32
	K []uint32
	W []uint32
	L []uint32
	R []uint32
	N uint32
}

func NewAnchorHash(buckets, used int) *AnchorHash {
	r := &AnchorHash{
		A: make([]uint32, buckets),
		K: make([]uint32, buckets),
		W: make([]uint32, buckets),
		L: make([]uint32, buckets),
		R: make([]uint32, buckets),
		N: uint32(used),
	}
	for i := uint32(0); i < uint32(used); i++ {
		r.K[i] = i
		r.W[i] = i
		r.L[i] = i
	}
	for i, j := uint32(buckets)-1, 0; i >= uint32(used); i, j = i-1, j+1 {
		r.A[i] = i
		r.R[j] = i
	}
	return r
}

func (r *AnchorHash) GetBucket(key uint64) uint32 {
	ha, hb, hc, hd := utils.FleaInit(key)
	b := utils.FastMod(uint64(hd), uint64(len(r.A)))
	for r.A[b] > 0 {
		ha, hb, hc, hd = utils.FleaRound(ha, hb, hc, hd)
		h := utils.FastMod(uint64(hd), uint64(r.A[b]))
		for r.A[h] >= r.A[b] {
			h = r.K[h]
		}
		b = h
	}
	return b
}

func (r *AnchorHash) GetPath(key uint64, pathBuffer []uint32) []uint32 {
	A, K := r.A, r.K
	ha, hb, hc, hd := utils.FleaInit(key)
	b := utils.FastMod(uint64(hd), uint64(len(A)))
	pathBuffer = append(pathBuffer, b)
	for A[b] > 0 {
		ha, hb, hc, hd = utils.FleaRound(ha, hb, hc, hd)
		h := utils.FastMod(uint64(hd), uint64(A[b]))
		pathBuffer = append(pathBuffer, h)
		for A[h] >= A[b] {
			h = K[h]
			pathBuffer = append(pathBuffer, h)
		}
		b = h
	}
	return pathBuffer
}

func (r *AnchorHash) AddBucket() uint32 {
	A, K, W, L, R, N := r.A, r.K, r.W, r.L, r.R, r.N
	b := R[len(R)-1]
	r.R = R[:len(R)-1]
	A[b] = 0
	L[W[N]] = N
	W[L[b]], K[b] = b, b
	r.N++
	return b
}

func (r *AnchorHash) RemoveBucket(b uint32) {
	if r.A[b] != 0 {
		return
	}
	r.N--
	A, K, W, L, N := r.A, r.K, r.W, r.L, r.N
	r.R = append(r.R, b)
	A[b] = N
	W[L[b]], K[b] = W[N], W[N]
	L[W[N]] = L[b]
}

func (r *AnchorHash) Print() {
	fmt.Printf("\nA: ")
	for _, item := range r.A {
		fmt.Printf("%v ", item)
	}
	fmt.Printf("\nK: ")
	for _, item := range r.K {
		fmt.Printf("%v ", item)
	}
	fmt.Printf("\nW: ")
	for _, item := range r.W {
		fmt.Printf("%v ", item)
	}
	fmt.Printf("\nL: ")
	for _, item := range r.L {
		fmt.Printf("%v ", item)
	}
	fmt.Printf("\nR: ")
	for _, item := range r.R {
		fmt.Printf("%v ", item)
	}
	fmt.Println()
}
