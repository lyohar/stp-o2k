module stpO2k

go 1.15

replace stpCommon => ../stp-common

require (
	github.com/confluentinc/confluent-kafka-go v1.5.2
	github.com/godror/godror v0.23.1
	github.com/google/uuid v1.1.5
	github.com/linkedin/goavro/v2 v2.9.7
	github.com/riferrei/srclient v0.2.1
	github.com/sirupsen/logrus v1.7.0
	github.com/spf13/viper v1.7.1
	stpCommon v0.0.0-00010101000000-000000000000
)
