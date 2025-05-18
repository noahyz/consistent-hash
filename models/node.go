package models

type HashNode interface {
	GetKey() string
	GetWeight() int
	IsEnabled() bool
	SetWeight(weight int)
	SetEnabled(isEnabled bool)
}

type NormalHashNode struct {
	key       string
	weight    int
	isEnabled bool
}

func NewNormalHashNode(key string, weight int, isEnabled bool) *NormalHashNode {
	return &NormalHashNode{
		key:       key,
		weight:    weight,
		isEnabled: isEnabled,
	}
}

func (r *NormalHashNode) GetKey() string {
	return r.key
}

func (r *NormalHashNode) GetWeight() int {
	return r.weight
}

func (r *NormalHashNode) IsEnabled() bool {
	return r.isEnabled
}

func (r *NormalHashNode) SetWeight(weight int) {
	r.weight = weight
}

func (r *NormalHashNode) SetEnabled(isEnabled bool) {
	r.isEnabled = isEnabled
}
