package main

import (
	"github.com/riferrei/srclient"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"stpCommon/model"
	"stpO2k/avroUtils"
	"stpO2k/client"
	"stpO2k/source"
	"stpO2k/target"
	"time"
)

func main() {
	logrus.SetFormatter(new(logrus.JSONFormatter))
	logrus.SetLevel(logrus.DebugLevel)
	logrus.Info("hello from stp-o2k")

	if err := initConfig(); err != nil {
		logrus.Fatal("could not load config file")
	}

	logrus.Debug("creating oracle source")
	logrus.Debug(viper.GetString("source.connection_string"))
	oracleSource, err := source.NewOracleSource(viper.GetString("source.connection_string"))
	if err != nil {
		logrus.Fatal("could not create oracle cource: ", err.Error())
	}
	logrus.Debug("created oracle source")

	cli := client.NewStpManagerClient(viper.GetString("manager.host"), viper.GetString("manager.port"))
	logrus.Debug("created manager client")

	schemaRegistryClient := srclient.CreateSchemaRegistryClient(viper.GetString("schema_registry.url"))
	logrus.Debug("created schema registry client")

	kafkaTarget, err := target.NewKafkaTarget(&target.KafkaTargetConfig{
		BootstrapServers: viper.GetString("target.bootstrap_servers"),
	})
	if err != nil {
		logrus.Fatal("could not create kafka target: ", err.Error())
	}
	logrus.Debug("created kafka target")

	avro := avroUtils.NewAvro(schemaRegistryClient)

	emptyCount := 0

	for {
		export, err := cli.GetExport()
		if err != nil {
			// TODO: count number of errors, and quit after max is exceeded
			logrus.Error("could not get export from manager: ", err.Error())
			break
		}

		if export.Command == model.ExportSkipCommand {
			logrus.Debug("got skip command, sleep")
			time.Sleep(time.Second * 5)
			continue
		} else if export.Command == model.ExportQuitCommand {
			logrus.Info("got quit command, exiting")
			break
		} else if export.Command == model.ExportExecuteCommand {
			empty, columns, err := oracleSource.SetExport(export)
			if err != nil {
				logrus.Error("could not set export for source: ", err.Error())
				break
				// what to do here?
			}

			if empty {
				//
				logrus.Debug("this export is empty, no work needed")
				// TODO switch o config parameter. move to manager
				status := &model.ExportStatus{
					TableSchema:        export.TableSchema,
					TableName:          export.TableName,
					OrderColumnToValue: export.OrderColumnFromValue,

				}

				err = cli.SetExportStatus(status)
				if err != nil {
					// TODO. what should i do here
					logrus.Error("could not set status")
				}
				if emptyCount == 5 {
					logrus.Debug("too often empty requests, sleep 2 seconds")
					time.Sleep(time.Second * 2)
				} else {
					emptyCount++
				}
				continue
			}

			emptyCount = 0

			schemaAndArgs, err := avro.GetJSONSchemaAndScanArgsByExportAndColumnsList(export, columns)
			if err != nil {
				//TODO what should o do here?
			}

			schemaId, err := avro.GetSchemaIdByJSONSchemaAndScanArgs(export, schemaAndArgs)
			if err != nil {
				logrus.Fatal(err.Error())
			}

			goAvroRows, err := oracleSource.GetRowsArrayInGoavro(schemaAndArgs.ScanArgs)
			if err != nil {
				logrus.Fatal(err.Error())
				// TODO error check
			}

			status, err := kafkaTarget.ExportGoAvroRowsToTarget(export, schemaId, schemaAndArgs.Schema, goAvroRows)
			if err != nil {
				logrus.Fatal(err.Error())
				// TODO error schek
			}

			err = cli.SetExportStatus(status)
			if err != nil {
				// TODO. what should i do here
				logrus.Fatal(err.Error())
			}
		} else {
			// should never be here
			logrus.Error("unknown command: ", export.Command)
			break
		}



	}


	logrus.Debug(oracleSource)
	logrus.Debug(cli)
}

func initConfig() error {
	viper.AddConfigPath("/Users/aryabov/projects/stp-main/stp-o2k/config")
	viper.SetConfigName("config")
	return viper.ReadInConfig()
}
