module github.com/coinexchain/ADS-benchmark

go 1.13

require (
	github.com/coinexchain/randsrc v0.1.0
	github.com/ethereum/go-ethereum v1.9.14
	github.com/tendermint/iavl v0.13.3
	github.com/tendermint/tm-db v0.5.1
)

replace github.com/coinexchain/randsrc => ../randsrc
