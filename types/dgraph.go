package types

type DgraphEpoch struct {
	Number   int
	Status   int
	Next     uint
	Previous uint
	Slots    uint
}

type DgraphSlot struct {
	Number      int
	Proposed_by uint
	Status      int
	RootHash    string
}

type DgraphValidator struct {
	Number           int
	EffectiveBalance uint
	CurrentBalance   uint
	Status           int
}
