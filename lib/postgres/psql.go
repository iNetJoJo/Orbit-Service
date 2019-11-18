package orbitpsql

import (
	pb "Orbit-Service/lib/proto/generated"
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jackc/pgx"
)

// DBPsql defines an object holding postgres related connection attributes
type DBPsql struct {
	conn *pgx.Conn
}

// Connect implements a functon for connecting to postgres database
func (db *DBPsql) Connect(DBaddress, DBuser, DBpassword, DBName, DBSSL string, DBport int) (*DBPsql, error) {
	ctx := context.Background()

	connString := db.genConnectionString(DBaddress, DBuser, DBpassword, DBName, DBSSL, DBport)

	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return nil, err
	}

	return &DBPsql{conn: conn}, nil
}

func (db *DBPsql) genConnectionString(DBaddress, DBuser, DBpassword, DBName, DBSSL string, DBport int) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		DBuser,
		DBpassword,
		DBaddress,
		DBport,
		DBName,
		DBSSL)
}

func (db *DBPsql) genStoredProcedureCall(call string) string {
	return "exec " + call
}

//DBCall implements generic grpc call related to postgres database call
func (db *DBPsql) DBCall(ctx context.Context, in *pb.DBCallRequest) (*pb.DBCallResponse, error) {

	var result map[string]interface{}

	rows, err := db.conn.Query(ctx, db.genStoredProcedureCall(in.Payload))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.DBCallResponse{Payload: []byte("test")}, nil
}

func PgSqlRowsToJson(rows *pgx.Rows) ([]byte, error) {
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
