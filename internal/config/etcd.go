package config

import (
	"context"
	"encoding/json"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"time"
)

// EtcdConfig Etcd 连接配置
type EtcdConfig struct {
	Endpoints []string      `json:"endpoints"`
	Timeout   time.Duration `json:"timeout"` // 单位：秒
	Username  string        `json:"username"`
	Password  string        `json:"password"`
}

// AppConfig 应用全局配置（支持热更新）
type AppConfig struct {
	Debug       bool   `json:"debug"`
	GRPCPort    int    `json:"grpc_port"`    // gRPC 服务端口
	HTTPPort    int    `json:"http_port"`    // Gin 网关端口
	ServiceName string `json:"service_name"` // 服务名称（gRPC 服务名）
}

var (
	Client       *clientv3.Client // Etcd 客户端实例
	GlobalConfig AppConfig        // 全局配置（热更新）
)

// InitEtcd 初始化 Etcd 客户端
func InitEtcd(conf EtcdConfig) error {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   conf.Endpoints,
		DialTimeout: conf.Timeout * time.Second,
		Username:    conf.Username,
		Password:    conf.Password,
	})
	if err != nil {
		log.Printf("Etcd 客户端初始化失败: %v", err)
		return err
	}

	// 验证连接
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = cli.Status(ctx, conf.Endpoints[0])
	if err != nil {
		log.Printf("Etcd 连接验证失败: %v", err)
		return err
	}

	Client = cli
	log.Println("Etcd 客户端初始化成功")
	return nil
}

// LoadConfig 从 Etcd 加载初始配置
func LoadConfig(configKey string) (AppConfig, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := Client.Get(ctx, configKey)
	if err != nil {
		log.Printf("加载 Etcd 配置失败: %v", err)
		return AppConfig{}, err
	}
	if len(resp.Kvs) == 0 {
		log.Printf("Etcd 配置不存在（key: %s），使用默认配置", configKey)
		return defaultConfig(), nil
	}

	// 解析 JSON 配置
	var conf AppConfig
	if err := json.Unmarshal(resp.Kvs[0].Value, &conf); err != nil {
		log.Printf("解析 Etcd 配置失败: %v", err)
		return defaultConfig(), err
	}

	GlobalConfig = conf
	log.Printf("加载配置成功: %+v", conf)
	return conf, nil
}

// WatchConfig 监听 Etcd 配置变更（热更新）
func WatchConfig(configKey string) {
	rch := Client.Watch(context.Background(), configKey)
	log.Printf("开始监听配置变更（key: %s）", configKey)

	for wresp := range rch {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case clientv3.EventTypePut:
				var newConf AppConfig
				if err := json.Unmarshal(ev.Kv.Value, &newConf); err != nil {
					log.Printf("解析变更配置失败: %v", err)
					continue
				}
				log.Printf("配置热更新: 旧配置=%+v → 新配置=%+v", GlobalConfig, newConf)
				GlobalConfig = newConf
			case clientv3.EventTypeDelete:
				log.Printf("配置被删除，恢复默认配置")
				GlobalConfig = defaultConfig()
			}
		}
	}
}

// defaultConfig 默认配置（配置不存在时使用）
func defaultConfig() AppConfig {
	return AppConfig{
		Debug:       true,
		GRPCPort:    50051,
		HTTPPort:    8080,
		ServiceName: "gin-grpc-etcd-service",
	}
}
