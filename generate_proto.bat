@echo off
protoc --go_out=./pkg/proto --go-grpc_out=./pkg/proto --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative api/service.proto
echo "gRPC 代码生成成功！"
pause