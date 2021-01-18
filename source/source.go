package source

import (
	"stpCommon/model"
)

type GoAvroRows []map[string]interface{}

type Source interface {
	SetExport(export *model.Export) error
}
