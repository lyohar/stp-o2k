package target

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/linkedin/goavro/v2"
	"github.com/sirupsen/logrus"
	"stpCommon/model"
	"stpO2k/source"
	"strings"
)

type KafkaTargetConfig struct {
	BootstrapServers string
}

type KafkaTarget struct {
	producer *kafka.Producer
}

func NewKafkaTarget(config *KafkaTargetConfig) (*KafkaTarget, error) {
	logrus.Debug("creating kafka producer")
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": config.BootstrapServers,
	})
	if err != nil {
		return nil, err
	}
	logrus.Debug("producer created")
	return &KafkaTarget{p}, nil
}

func (t *KafkaTarget) ExportGoAvroRowsToTarget(export *model.Export, schemaId int, schema string, rows source.GoAvroRows) (*model.ExportStatus, error)  {

	// TODO reformat it
	go func() {
		for e := range t.producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	status := &model.ExportStatus{
		TableSchema:        export.TableSchema,
		TableName:          export.TableName,
		OrderColumnToValue: "",
	}

	keysMap := make(map[string]int)
	for i, k := range strings.Split(export.KeysList, ",") {
		keysMap[k] = i
	}
	if len(keysMap) == 0 {
		return nil, errors.New(fmt.Sprintf("keys should not be empty"))
	}


	columnsMap := make(map[string]int)
	for i, k := range strings.Split(export.ColumnsList, ",") {
		columnsMap[k] = i
	}
	if len(columnsMap) == 0 {
		return nil, errors.New(fmt.Sprintf("columns should not be empty"))
	}


	for _, row := range rows{
		outRow := map[string]interface{}{}
		outKey := map[string]interface{}{}

		var (
			timestampFound, operationFound bool
			timestamp int64
			operation string
			)


		for columnName, column := range row {

			//logrus.Debug("kafka target. column_name: ", columnName, " column_value: ", column)

			_, ok := columnsMap[columnName]
			if !ok {
				return nil, errors.New("column not found")
			}
			outRow[columnName] = column

			// TODO. should i really check all types in key columns or just passing a field is enough?
			_, ok = keysMap[columnName]
			if ok {
				outKey[columnName] = column
			}

			if columnName == export.TimestampColumnName {
				timestampFound = true
				timestamp, ok = column.(int64)
				if !ok {
					timestamp, ok = column.(map[string]interface{})["long"].(int64)
					if !ok {
						return nil, errors.New("could not get timestamp column value")
					}
				}
			}

			if columnName == export.OperationColumnName {
				operationFound = true
				operation, ok = column.(string)
				if !ok {
					operation, ok = column.(map[string]interface{})["string"].(string)
					if !ok {
						return nil, errors.New("could not get operation column value")
					}
				}
			}

			// TODO. should order column value always be string?
			if columnName == export.OrderColumnName {
				status.OrderColumnToValue, ok = column.(string)
				if !ok {
					status.OrderColumnToValue, ok = column.(map[string]interface{})["string"].(string)
					if !ok {
						return nil, errors.New("could not get order column value")
					}

				}

			}
		}

		if !timestampFound {
			return nil, errors.New("timestamp column not found")
		}

		if !operationFound {
			return nil, errors.New("operation column not found")
		}

		out := map[string]interface{}{
			"system_info": map[string]interface{}{
				"timestamp": timestamp,
				"operation": operation,
			},
		}

		out["payload"] = goavro.Union(fmt.Sprintf("%s.payload", export.TableSchema), outRow)
		out["key"] = outKey

		codec, err := goavro.NewCodec(schema)
		if err != nil {
			return nil, err
		}

		valueBytes, err :=  codec.BinaryFromNative(nil, out)
		if err != nil {
			return nil, err
		}

		schemaIDBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(schemaIDBytes, uint32(schemaId))

		var recordValue []byte
		recordValue = append(recordValue, byte(0))
		recordValue = append(recordValue, schemaIDBytes...)
		recordValue = append(recordValue, valueBytes...)

		q, _ := uuid.NewUUID()
		t.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &export.TopicName, Partition: kafka.PartitionAny},
			Key: []byte(q.String()), Value: recordValue}, nil)
	}

	return status, nil

}
