package main

import (
	"context"
	"flag"
	"fmt"
	proto "gin+grpc+etcd/pkg/proto/api"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"gin+grpc+etcd/internal/config"
	"gin+grpc+etcd/internal/discovery"
	"gin+grpc+etcd/internal/service"
)

// 命令行参数
var (
	etcdEndpoints = flag.String("etcd-endpoints", "127.0.0.1:2379", "Etcd 节点地址（逗号分隔）")
	nodeIP        = flag.String("node-ip", "127.0.0.1", "当前节点 IP")
	grpcPort      = flag.Int("grpc-port", 50051, "gRPC 服务端口（默认 50051）")
	configKey     = flag.String("config-key", "gin-grpc-etcd-service", "Etcd 配置 key")
)

func main() {
	flag.Parse()

	// 1. 初始化 Etcd 客户端
	err := config.InitEtcd(config.EtcdConfig{
		Endpoints: strings.Split(*etcdEndpoints, ","),
		Timeout:   5, // 5 秒超时
		// Username: "admin", // 可选：Etcd 认证
		// Password: "123456",
	})
	if err != nil {
		log.Fatalf("Etcd 初始化失败：%v", err)
	}
	defer config.Client.Close()

	// 2. 加载配置（支持热更新）
	appconf, err := config.LoadConfig(*configKey)
	if err != nil {
		log.Fatalf("加载配置失败：%v", err)
	}
	go config.WatchConfig(*configKey) // 后台监听配置变更

	// 3. 初始化服务注册
	registry := discovery.NewServiceRegistry(config.Client, appconf.ServiceName)

	// 4. 启动 gRPC 服务
	nodeAddr := fmt.Sprintf("%s:%d", *nodeIP, *grpcPort)
	lis, err := net.Listen("tcp", nodeAddr)
	if err != nil {
		log.Fatalf("gRPC 监听失败（%s）：%v", nodeAddr, err)
	}

	// 创建 gRPC 服务器（可添加拦截器：日志、认证、限流等）
	s := grpc.NewServer()

	// 注册业务服务
	proto.RegisterTaskServiceServer(s, service.NewTaskServiceImpl(nodeAddr))

	// 注册健康检查服务（可选，用于服务健康探测）
	grpc_health_v1.RegisterHealthServer(s, health.NewServer())

	// 5. 注册服务到 Etcd（租约 30 秒）
	if err := registry.Register(nodeAddr, 30); err != nil {
		log.Fatalf("服务注册失败：%v", err)
	}
	defer registry.Unregister(nodeAddr) // 程序退出时注销

	// 6. 启动 gRPC 服务（异步）
	go func() {
		log.Printf("gRPC 服务启动成功，监听地址：%s", nodeAddr)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("gRPC 服务启动失败：%v", err)
		}
	}()

	// 7. 优雅关闭
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("开始关闭 gRPC 服务...")

	// 关闭 gRPC 服务（5 秒超时）
	_, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.GracefulStop()

	log.Println("gRPC 服务已正常关闭")
}
