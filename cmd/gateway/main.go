package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"gin+grpc+etcd/internal/config"
	"gin+grpc+etcd/internal/discovery"
	"gin+grpc+etcd/internal/gateway"
)

var (
	etcdEndpoints = flag.String("etcd-endpoints", "127.0.0.1:2379", "Etcd 节点地址（逗号分隔）")
	configKey     = flag.String("config-key", "gin-grpc-etcd-service", "Etcd 配置 key")
)

func main() {
	flag.Parse()

	// 1. 初始化 Etcd 客户端
	err := config.InitEtcd(config.EtcdConfig{
		Endpoints: strings.Split(*etcdEndpoints, ","),
		Timeout:   5,
	})
	if err != nil {
		log.Fatalf("Etcd 初始化失败：%v", err)
	}
	defer config.Client.Close()

	// 2. 加载配置（支持热更新）
	conf, err := config.LoadConfig(*configKey)
	if err != nil {
		log.Fatalf("加载配置失败：%v", err)
	}
	go config.WatchConfig(*configKey)

	// 3. 初始化服务发现
	registry := discovery.NewServiceRegistry(config.Client, conf.ServiceName)

	// 4. 初始化 Gin 网关处理器
	gin.SetMode(gin.ReleaseMode)
	if conf.Debug {
		gin.SetMode(gin.DebugMode)
	}
	r := gin.Default()

	// 5. 初始化网关处理器（传入服务发现实例）
	gatewayHandler := gateway.NewGatewayHandler(registry)

	// 关键：启动节点监听（后台协程，持续监听节点变更）
	go registry.WatchNodes(gatewayHandler.UpdateNodeCache)

	// 6. 注册 HTTP 路由
	api := r.Group("/api/v1")
	{
		api.POST("/task/process", gatewayHandler.ProcessTaskHTTP)
		api.POST("/task/batch-process", gatewayHandler.BatchProcessTaskHTTP)
	}

	// 7. 启动 Gin 服务（后续代码不变）
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", conf.HTTPPort),
		Handler: r,
	}

	go func() {
		log.Printf("Gin 网关启动成功，监听地址：:%d", conf.HTTPPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Gin 服务启动失败：%v", err)
		}
	}()

	// 优雅关闭（后续代码不变）
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("开始关闭 Gin 网关...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Gin 网关关闭失败：%v", err)
	}

	log.Println("Gin 网关已正常关闭")
}
