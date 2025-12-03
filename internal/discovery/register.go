package discovery

import (
	"context"
	"encoding/json"
	"log"
	"path"
	"time"

	"go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
)

// ServiceRegistry 服务注册发现结构体
type ServiceRegistry struct {
	client     *clientv3.Client
	serviceKey string           // 服务根路径（如 /services/gin-grpc-service）
	leaseID    clientv3.LeaseID // 租约 ID（服务健康检测）
}

// NewServiceRegistry 创建注册实例
func NewServiceRegistry(cli *clientv3.Client, serviceName string) *ServiceRegistry {
	return &ServiceRegistry{
		client:     cli,
		serviceKey: path.Join("/services", serviceName),
	}
}

// Register 注册 gRPC 服务节点（带租约自动续期）
func (r *ServiceRegistry) Register(nodeAddr string, ttl int64) error {
	// 1. 创建租约（服务下线后自动注销）
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := r.client.Grant(ctx, ttl)
	if err != nil {
		log.Printf("创建 Etcd 租约失败: %v", err)
		return err
	}
	r.leaseID = resp.ID

	// 2. 注册节点（绑定租约）
	nodeKey := path.Join(r.serviceKey, nodeAddr) // 节点唯一标识
	em, err := endpoints.NewManager(r.client, r.serviceKey)
	if err != nil {
		log.Printf("创建节点管理器失败: %v", err)
		return err
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if err := em.AddEndpoint(ctx2, nodeKey, endpoints.Endpoint{Addr: nodeAddr}, clientv3.WithLease(r.leaseID)); err != nil {
		log.Printf("注册服务节点失败（addr: %s）: %v", nodeAddr, err)
		return err
	}

	// 3. 租约自动续期（后台协程）
	go r.keepAlive()

	log.Printf("gRPC 服务节点注册成功（addr: %s, serviceKey: %s）", nodeAddr, r.serviceKey)
	return nil
}

// keepAlive 租约续期
func (r *ServiceRegistry) keepAlive() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := r.client.KeepAlive(ctx, r.leaseID)
	if err != nil {
		log.Printf("租约续期失败: %v", err)
		return
	}

	for resp := range ch {
		log.Printf("租约续期成功（leaseID: %d, ttl: %d）", resp.ID, resp.TTL)
	}
	log.Printf("租约续期停止（leaseID: %d）", r.leaseID)
}

// Unregister 注销服务节点
func (r *ServiceRegistry) Unregister(nodeAddr string) error {
	nodeKey := path.Join(r.serviceKey, nodeAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 删除节点
	_, err := r.client.Delete(ctx, nodeKey)
	if err != nil {
		log.Printf("注销节点失败（addr: %s）: %v", nodeAddr, err)
		return err
	}

	// 撤销租约
	if _, err := r.client.Revoke(ctx, r.leaseID); err != nil {
		log.Printf("撤销租约失败（leaseID: %d）: %v", r.leaseID, err)
		return err
	}

	log.Printf("服务节点注销成功（addr: %s）", nodeAddr)
	return nil
}

func (r *ServiceRegistry) Discover() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 前缀查询服务节点
	resp, err := r.client.Get(ctx, r.serviceKey, clientv3.WithPrefix())
	if err != nil {
		log.Printf("发现服务节点失败: %v", err)
		return nil, err
	}

	var nodes []string
	for _, kv := range resp.Kvs {
		// 定义与 Endpoint 结构一致的本地结构体（避免依赖官方包的变更）
		type LocalEndpoint struct {
			Addr string `json:"addr"` // 注意字段名小写+json tag，与存储格式一致
		}
		var ep LocalEndpoint
		// 手动用 JSON 反序列化（替代原来的 ep.Unmarshal）
		if err := json.Unmarshal(kv.Value, &ep); err != nil {
			log.Printf("解析节点信息失败: %v, 原始数据: %s", err, string(kv.Value))
			continue
		}
		if ep.Addr != "" { // 过滤空地址
			nodes = append(nodes, ep.Addr)
		}
	}

	log.Printf("发现服务节点: %v", nodes)
	return nodes, nil
}

// WatchNodes 监听节点变更（动态感知节点上下线）
func (r *ServiceRegistry) WatchNodes(callback func(nodes []string)) {
	rch := r.client.Watch(context.Background(), r.serviceKey, clientv3.WithPrefix())
	log.Printf("开始监听节点变更（serviceKey: %s）", r.serviceKey)

	for wresp := range rch {
		if wresp.Err() != nil {
			log.Printf("节点监听错误: %v", wresp.Err())
			continue
		}
		// 节点变更后重新查询所有节点
		nodes, err := r.Discover()
		if err != nil {
			log.Printf("节点变更后查询失败: %v", err)
			continue
		}
		callback(nodes)
	}
}
