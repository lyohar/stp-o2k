package source

import (
	"database/sql"
	"errors"
	"fmt"
	"stpCommon/model"

	"github.com/linkedin/goavro/v2"
	"github.com/sirupsen/logrus"

	_ "github.com/godror/godror"
)

type OracleSource struct {
	ConnectionString string
	db               *sql.DB
	rows             *sql.Rows
	ct               []*sql.ColumnType
}



func NewOracleSource(connectionString string) (*OracleSource, error) {
	logrus.Debug("connecting to oracle")
	db, err := sql.Open("godror", connectionString)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	logrus.Debug("connected to oracle")
	return &OracleSource{
		ConnectionString: connectionString,
		db:               db,
	}, nil
}

func getExportQuery(export *model.Export) string {
	var result string
	result = fmt.Sprintf("select /*+index(t)*/ %s from %s.%s t ", export.ColumnsList, export.TableSchema, export.TableName)
	if export.OrderColumnFromValue != "" {
		result += fmt.Sprintf(" where %s > :val ", export.OrderColumnName)
	}
	result += fmt.Sprintf(" order by %s", export.OrderColumnName)
	result += fmt.Sprintf(" fetch next %d rows only", 10000)
	//logrus.Debug("generated query: ", result)
	return result
}

func getMaxOrderColumnQuery(export *model.Export) string {
	return fmt.Sprintf("select max(%s) from %s.%s", export.OrderColumnName, export.TableSchema, export.TableName)
}

func (s *OracleSource) SetExport(export *model.Export) (bool, []*sql.ColumnType, error) {
	//if s.rows != nil {
	//	return true, nil, errors.New("rows are not nil. looks like prevoius export is still executing")
	//}

	var err error

	// check if there are rows in the exported table
	maxOrderColumnValueQuery :=  getMaxOrderColumnQuery(export)
	var maxOrderColumnValue sql.NullString
	err = s.db.QueryRow(maxOrderColumnValueQuery).Scan(&maxOrderColumnValue)

	// the table is empty, do the export is also empty
	if err == sql.ErrNoRows {
		return true, nil, nil
	}

	if err != nil {
		return false, nil, errors.New(fmt.Sprintf("could not check max order query value: %s", err.Error()))
	}

	// max order column value less than already exported - export is empty
	if maxOrderColumnValue.String <= export.OrderColumnFromValue {
		return true, nil, nil
	}

	query := getExportQuery(export)

	if export.OrderColumnFromValue != "" {
		s.rows, err = s.db.Query(query, export.OrderColumnFromValue)
	} else {
		s.rows, err = s.db.Query(query)
	}

	//if err == sql.ErrNoRows {
	//	logrus.Debug("there are no rows for this export")
	//	if s.rows != nil {
	//		_ = s.rows.Close()
	//	}
	//	return true, nil, nil
	//}

	ct, err := s.rows.ColumnTypes()
	if err != nil {
		logrus.Error("could not get column types: ", err.Error())
		return true, nil, err
	}
	s.ct = ct
	return false, ct, nil
}

func (s *OracleSource) GetRowsArrayInGoavro(scanArgs []interface{}) (GoAvroRows, error) {
	var result []map[string]interface{}
	for s.rows.Next() {
		err := s.rows.Scan(scanArgs...)

		if err != nil {
			return nil, err
		}

		row := map[string]interface{}{}

		for i, v := range s.ct {

			nullable, ok := v.Nullable()
			if !ok {
				return nil, errors.New("could not determine if field is nullable")
			}

			// TODO. switch to type switch? :-)
			if z, ok := (scanArgs[i]).(*sql.NullBool); ok {
				if nullable {
					row[v.Name()] = goavro.Union("boolean", z.Bool)
				} else {
					row[v.Name()] = z.Bool
				}
				continue
			}

			if z, ok := (scanArgs[i]).(*sql.NullString); ok {
				if nullable {
					if z.String == "" {
						row[v.Name()] = nil
					} else {
						row[v.Name()] = goavro.Union("string", z.String)
					}
				} else {
					row[v.Name()] = z.String
				}
				//logrus.Debug("oracle source. column_name: ", v.Name(), " value: ", row[v.Name()])
				continue
			}

			if z, ok := (scanArgs[i]).(*sql.NullInt64); ok {
				if nullable {
					row[v.Name()] = goavro.Union("int", z.Int64)
				} else {
					row[v.Name()] = z.Int64
				}
				continue
			}

			if z, ok := (scanArgs[i]).(*sql.NullFloat64); ok {
				if nullable {
					row[v.Name()] = goavro.Union("float", z.Float64)
				} else {
					row[v.Name()] = z.Float64
				}
				continue
			}

			if z, ok := (scanArgs[i]).(*sql.NullTime); ok {
				if nullable {
					row[v.Name()] = goavro.Union("long", z.Time.Unix())
				} else {
					row[v.Name()] = z.Time.Unix()
				}
				continue
			}

			// should i panic here?
			row[v.Name()] = scanArgs[i]
		}
		result = append(result, row)
	}
	return result, nil
}
