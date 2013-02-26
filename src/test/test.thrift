namespace java com.github.kdlan.thrift.zeromq.service

service EchoService{

	string echo(1:string msg)
}
