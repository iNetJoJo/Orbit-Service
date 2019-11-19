package orbitpsql

import (
	pb "Orbit-Service/lib/proto/generated"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

// OrbitPSQL defines an object holding postgres related connection attributes
type OrbitPSQL struct {
	conn *gorm.DB
}

// Connect implements a functon for connecting to postgres database
func (db *OrbitPSQL) Connect(DBaddress, DBuser, DBpassword, DBName, DBSSL string, DBport int) (*OrbitPSQL, error) {

	connString := db.genConnectionString(DBaddress, DBuser, DBpassword, DBName, DBSSL, DBport)

	conn, err := gorm.Open("postgres", connString)
	if err != nil {
		return nil, err
	}

	return &OrbitPSQL{conn: conn}, nil
}

func (db *OrbitPSQL) genConnectionString(DBaddress, DBuser, DBpassword, DBName, DBSSL string, DBport int) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		DBuser,
		DBpassword,
		DBaddress,
		DBport,
		DBName,
		DBSSL)
}

func (db *OrbitPSQL) genStoredProcedureCall(call string) string {
	return "exec " + call
}

//DBCall implements generic grpc call related to postgres database call
func (db *OrbitPSQL) DBCall(ctx context.Context, in *pb.DBCallRequest) (*pb.DBCallResponse, error) {
	rows, err := db.conn.Raw(db.genStoredProcedureCall(in.Payload)).Rows()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	result, err := PgSqlRowsToJson(rows)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.DBCallResponse{Payload: result}, nil
}

func PgSqlRowsToJson(rows *sql.Rows) ([]byte, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	count := len(columns)
	tableData := make([]map[string]interface{}, 0)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	for rows.Next() {
		for i := 0; i < count; i++ {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			entry[col] = v
		}
		tableData = append(tableData, entry)
	}
	jsonData, err := json.Marshal(tableData)
	if err != nil {
		return nil, err
	}

	return jsonData, nil
}
