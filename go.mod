module soloos/solodb

go 1.13

require (
	github.com/google/flatbuffers v1.11.0
	github.com/mattn/go-sqlite3 v1.11.0
	github.com/stretchr/testify v1.3.0
	soloos/common v0.0.0
	soloos/soloboat v0.0.0
)

replace (
	soloos/common v0.0.0 => /soloos/common
	soloos/soloboat v0.0.0 => /soloos/soloboat
	soloos/solodb v0.0.0 => /soloos/solodb
	soloos/solofs v0.0.0 => /soloos/solofs
	soloos/solomq v0.0.0 => /soloos/solomq
)
