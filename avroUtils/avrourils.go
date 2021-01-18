package avroUtils
//TODO rename

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/riferrei/srclient"
	"github.com/sirupsen/logrus"
	"stpCommon/model"
	"strings"
)

type JSONSchemaAndScanArgs struct {
	Schema string
	ScanArgs []interface{}
}

type Avro struct {
	srClient *srclient.SchemaRegistryClient
}

func NewAvro(srClient *srclient.SchemaRegistryClient) *Avro {
	return &Avro{
		srClient: srClient,
	}
}

func createKetsMapFromString(keys string) (map[string]int, error)  {
	// TODO process keys correctly
	keysMap := make(map[string]int)

	for i, k := range strings.Split(keys, ",") {
		keysMap[k] = i
	}
	if len(keys) == 0 {
		return nil, errors.New(fmt.Sprintf("keys should not be empty"))
	}
	logrus.Debug("keys:", keys)
	return keysMap, nil

}


func (a *Avro) GetJSONSchemaAndScanArgsByExportAndColumnsList(export *model.Export, columns  []*sql.ColumnType)  (*JSONSchemaAndScanArgs, error) {

	scanArgs := make([]interface{}, len(columns))

	resMap := make(map[string]interface{})
	resMap["type"] = "record"
	resMap["name"] = export.TableName
	resMap["namespace"] = export.TableSchema
	resMap["fields"] = []interface{}{}

	sysinfoMap := map[string]interface{}{
		"name": "system_info",
		"type": map[string]interface{}{
			"type": "record",
			"name": "sysinfo",
			"fields": []interface{}{
				map[string]string{
					"name": "operation",
					"type": "string",
				},
				map[string]interface{}{
					"name": "timestamp",
					"type": map[string]string{
						"type": "long",
						"logical_type": "timestamp_millis",
					},
				},
			},
		},
	}

	keyMap := map[string]interface{}{
		"name": "key",
		"type": map[string]interface{}{
			"type": "record",
			"name": "key",
			"fields": []interface{}{},
		},
	}

	keys, err := createKetsMapFromString(export.KeysList)
	if err != nil {
		return nil, err
	}

	payloadMap := map[string]interface{} {
		"name": "payload",
		"default": nil,
		"type": []interface{}{
			"null",
		},
	}

	payloadMapRecord := make(map[string]interface{})
	payloadMapRecord["type"] = "record"
	payloadMapRecord["name"] = "payload"
	payloadMapRecord["fields"] = []interface{}{}

	for i, c := range columns {
		nullable, ok := c.Nullable()
		if !ok {
			return nil, errors.New("not supported")
		}

		var currentColType interface{}
		switch c.DatabaseTypeName() {
		case "CHAR", "VARCHAR", "VARCHAR2", "NVARCHAR2":
			scanArgs[i] = new(sql.NullString)
			currentColType = "string"

		case "NUMBER":
			_, s, ok := c.DecimalSize()
			if !ok {
				return nil, errors.New("could not get precision and scale")
			}
			if s == 0 {
				scanArgs[i] = new(sql.NullInt64)
				currentColType = "int"
			} else {
				scanArgs[i] = new(sql.NullFloat64)
				currentColType = "float"
			}
		case "DATE", "TIMESTAMP":
			scanArgs[i] = new(sql.NullTime)
			currentColType = &map[string]string {
				"type": "long",
				"logical_type": "timestamp_millis",
			}

		case "BOOL":
			scanArgs[i] = new(sql.NullBool)
			currentColType = "bool"

		default:
			return nil, errors.New(fmt.Sprintf("unsupported type: ", c.DatabaseTypeName()))
		}

		currentCol := map[string]interface{}{}
		currentCol["name"] = c.Name()

		if !nullable {
			currentCol["type"] = currentColType
		} else {
			currentCol["default"] = nil
			currentCol["type"] = []interface{}{
				"null",
				currentColType,
			}
		}
		payloadMapRecord["fields"] = append(payloadMapRecord["fields"].([]interface{}), currentCol)

		_, ok = keys[c.Name()]
		if ok {
			if nullable {
				return nil, errors.New(fmt.Sprintf("key should not be nullable"))
			}
			keyMap["type"].(map[string]interface{})["fields"] = append(keyMap["type"].(map[string]interface{})["fields"].([]interface{}), currentCol)
		}
	}

	payloadMap["type"] = append(payloadMap["type"].([]interface{}), payloadMapRecord)
	resMap["fields"] = append(resMap["fields"].([]interface{}), sysinfoMap)
	resMap["fields"] = append(resMap["fields"].([]interface{}), keyMap)
	resMap["fields"] = append(resMap["fields"].([]interface{}), payloadMap)
	r, err := json.Marshal(resMap)
	if err != nil {
		return nil, err
	}
	return &JSONSchemaAndScanArgs{Schema: string(r), ScanArgs: scanArgs}, nil
}

func (a *Avro) GetSchemaIdByJSONSchemaAndScanArgs(export *model.Export, schema *JSONSchemaAndScanArgs) (int, error)  {
	s, err := a.srClient.CreateSchema(export.TopicName, schema.Schema, srclient.Avro, false)
	if err != nil {
		return 0, err
	}
	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(s.ID()))
	logrus.Debug("the id of schema: ", s.ID())
	return s.ID(), nil
}
