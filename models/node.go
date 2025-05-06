package models

type HashNode interface {
	GetKey() string
	GetWeight() int
}
