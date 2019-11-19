package orbit

import (
	orbitpsql "Orbit-Service/lib/postgres"
	pb "Orbit-Service/lib/proto/generated"
	"google.golang.org/grpc"
)

type OrbitServer struct{
	PSQL orbitpsql.OrbitPSQL
}

func (o *OrbitServer) Initialize() (*OrbitServer, error){
	serv := grpc.NewServer()

	pb.RegisterPostGresServer(serv, o.PSQL)
	
	return o, nil
}