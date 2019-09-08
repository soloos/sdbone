module soloos/sdbone

go 1.12

require (
	github.com/google/flatbuffers v1.11.0
	github.com/mattn/go-colorable v0.1.2 // indirect
	github.com/mattn/go-isatty v0.0.9 // indirect
	github.com/stretchr/testify v1.3.0
	soloos/common v0.0.0
	soloos/soloboat v0.0.0
)

replace (
	soloos/common v0.0.0 => /soloos/common
	soloos/sdbone v0.0.0 => /soloos/sdbone
	soloos/sdfs v0.0.0 => /soloos/sdfs
	soloos/soloboat v0.0.0 => /soloos/soloboat
	soloos/swal v0.0.0 => /soloos/swal
)
