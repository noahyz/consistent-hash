package utils

import "github.com/spaolacci/murmur3"

const DefaultHashSeedNum = 192

func GetHashCode(key []byte) uint64 {
	return murmur3.Sum64WithSeed(key, DefaultHashSeedNum)
}
