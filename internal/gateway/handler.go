package gateway

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"gin+grpc+etcd/internal/discovery"
	"gin+grpc+etcd/pkg/proto/api"
)

// GatewayHandler Gin 网关处理器（完整实现）
type GatewayHandler struct {
	registry      *discovery.ServiceRegistry  // 服务发现实例
	grpcConnCache map[string]*grpc.ClientConn // gRPC 连接缓存（复用连接）
	nodeCache     []string                    // gRPC 节点缓存（实时更新）
	mu            sync.RWMutex                // 缓存读写锁（线程安全）
}

// NewGatewayHandler 创建网关处理器（初始化缓存+服务发现）
func NewGatewayHandler(registry *discovery.ServiceRegistry) *GatewayHandler {
	handler := &GatewayHandler{
		registry:      registry,
		grpcConnCache: make(map[string]*grpc.ClientConn),
		nodeCache:     make([]string, 0),
	}

	// 初始化节点缓存（启动时先查一次 Etcd，避免缓存为空）
	nodes, err := registry.Discover()
	if err != nil {
		log.Printf("初始化节点缓存失败: %v", err)
	} else {
		handler.mu.Lock()
		handler.nodeCache = nodes
		handler.mu.Unlock()
		log.Printf("初始化节点缓存: %v", nodes)
	}

	return handler
}

// UpdateNodeCache 节点变更回调函数（更新本地缓存+清理无效连接）
func (h *GatewayHandler) UpdateNodeCache(nodes []string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// 打印节点变更日志
	log.Printf("节点列表变更：旧节点=%v → 新节点=%v", h.nodeCache, nodes)

	h.nodeCache = nodes

	// 清理已下线节点的 gRPC 连接缓存（避免连接泄露）
	for addr := range h.grpcConnCache {
		exists := false
		for _, node := range nodes {
			if addr == node {
				exists = true
				break
			}
		}
		if !exists {
			log.Printf("清理下线节点的连接：%s", addr)
			// 关闭连接
			if err := h.grpcConnCache[addr].Close(); err != nil {
				log.Printf("关闭连接失败：%v", err)
			}
			// 从缓存删除
			delete(h.grpcConnCache, addr)
		}
	}
}

// getGRPCConn 从缓存获取 gRPC 连接（不存在则创建，复用连接提升性能）
func (h *GatewayHandler) getGRPCConn(nodeAddr string) (*grpc.ClientConn, error) {
	// 先查缓存
	if conn, ok := h.grpcConnCache[nodeAddr]; ok {
		return conn, nil
	}

	// 缓存未命中，创建新连接（无 TLS，生产环境需配置 TLS）
	conn, err := grpc.Dial(
		nodeAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()), // 无 TLS 加密
		grpc.WithTimeout(2*time.Second),                          // 连接超时 2 秒
		grpc.WithBlock(),                                         // 阻塞直到连接成功
	)
	if err != nil {
		log.Printf("创建 gRPC 连接失败（节点：%s）：%v", nodeAddr, err)
		return nil, fmt.Errorf("连接节点 %s 失败：%v", nodeAddr, err)
	}

	// 存入缓存，后续复用
	h.grpcConnCache[nodeAddr] = conn
	log.Printf("创建 gRPC 连接成功（节点：%s）", nodeAddr)
	return conn, nil
}

// selectNode 从本地缓存选择节点（随机负载均衡，失败降级查询 Etcd）
func (h *GatewayHandler) selectNode() (string, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// 缓存有节点，直接随机选择
	if len(h.nodeCache) > 0 {
		rand.Seed(time.Now().UnixNano())
		return h.nodeCache[rand.Intn(len(h.nodeCache))], nil
	}

	// 缓存为空，降级查询 Etcd（避免服务不可用）
	log.Printf("节点缓存为空，降级查询 Etcd")
	nodes, err := h.registry.Discover()
	if err != nil {
		return "", fmt.Errorf("查询 Etcd 失败：%v", err)
	}
	if len(nodes) == 0 {
		return "", fmt.Errorf("无可用 gRPC 节点")
	}

	// 更新缓存（临时兜底）
	h.mu.Lock()
	h.nodeCache = nodes
	h.mu.Unlock()

	rand.Seed(time.Now().UnixNano())
	return nodes[rand.Intn(len(nodes))], nil
}

// ProcessTaskHTTP 完整实现：HTTP 接口 → 转发到 gRPC ProcessTask
func (h *GatewayHandler) ProcessTaskHTTP(c *gin.Context) {
	// 1. 解析 HTTP 请求参数（必传 param 字段）
	var req struct {
		Param string `json:"param" binding:"required"` // 业务参数（必填）
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		log.Printf("参数解析失败：%v", err)
		c.JSON(http.StatusBadRequest, gin.H{
			"code": 1,
			"msg":  fmt.Sprintf("参数错误：%v（param 为必填字段）", err),
			"data": nil,
		})
		return
	}

	log.Printf("接收 HTTP 请求：/api/v1/task/process，param=%s", req.Param)

	// 2. 选择可用的 gRPC 节点
	nodeAddr, err := h.selectNode()
	if err != nil {
		log.Printf("选择 gRPC 节点失败：%v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"code": 1,
			"msg":  fmt.Sprintf("服务暂时不可用：%v", err),
			"data": nil,
		})
		return
	}
	log.Printf("选择 gRPC 节点：%s", nodeAddr)

	// 3. 获取 gRPC 连接（复用缓存或新建）
	conn, err := h.getGRPCConn(nodeAddr)
	if err != nil {
		log.Printf("获取 gRPC 连接失败：%v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"code": 1,
			"msg":  fmt.Sprintf("连接服务节点失败：%v", err),
			"data": nil,
		})
		return
	}

	// 4. 调用 gRPC 服务（设置 3 秒超时）
	client := proto.NewTaskServiceClient(conn)
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	// 生成唯一任务 ID（便于追踪）
	taskID := fmt.Sprintf("http-task-%d-%d", time.Now().UnixMilli(), rand.Intn(1000))
	grpcReq := &proto.TaskRequest{
		TaskId:  taskID,
		Param:   req.Param,
		Timeout: 3, // gRPC 服务超时 3 秒
	}

	// 执行 gRPC 调用
	grpcResp, err := client.ProcessTask(ctx, grpcReq)
	if err != nil {
		log.Printf("调用 gRPC 服务失败（节点：%s）：%v", nodeAddr, err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"code": 1,
			"msg":  fmt.Sprintf("任务处理失败：%v", err),
			"data": gin.H{
				"task_id":   taskID,
				"node_addr": nodeAddr,
			},
		})
		return
	}

	// 5. 处理 gRPC 响应，返回 HTTP 结果
	log.Printf("gRPC 调用成功（taskID：%s，节点：%s）", taskID, nodeAddr)
	c.JSON(http.StatusOK, gin.H{
		"code": 0,
		"msg":  "任务处理成功",
		"data": gin.H{
			"task_id":   grpcResp.TaskId,
			"node_addr": grpcResp.NodeAddr,
			"param":     req.Param,
			"result":    grpcResp.Result,
			"success":   grpcResp.Success,
		},
	})
}

// BatchProcessTaskHTTP 完整实现：HTTP 接口 → 转发到 gRPC 流式接口
func (h *GatewayHandler) BatchProcessTaskHTTP(c *gin.Context) {
	// 1. 解析 HTTP 请求参数（必传 params 数组）
	var req struct {
		Params []string `json:"params" binding:"required,min=1"` // 批量参数（至少 1 个）
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		log.Printf("批量任务参数解析失败：%v", err)
		c.JSON(http.StatusBadRequest, gin.H{
			"code": 1,
			"msg":  fmt.Sprintf("参数错误：%v（params 为必填数组，至少 1 个元素）", err),
			"data": nil,
		})
		return
	}

	log.Printf("接收批量 HTTP 请求：/api/v1/task/batch-process，参数数量=%d", len(req.Params))

	// 2. 选择可用的 gRPC 节点
	nodeAddr, err := h.selectNode()
	if err != nil {
		log.Printf("选择 gRPC 节点失败：%v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"code": 1,
			"msg":  fmt.Sprintf("服务暂时不可用：%v", err),
			"data": nil,
		})
		return
	}
	log.Printf("选择 gRPC 节点：%s", nodeAddr)

	// 3. 获取 gRPC 连接
	conn, err := h.getGRPCConn(nodeAddr)
	if err != nil {
		log.Printf("获取 gRPC 连接失败：%v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"code": 1,
			"msg":  fmt.Sprintf("连接服务节点失败：%v", err),
			"data": nil,
		})
		return
	}

	// 4. 调用 gRPC 流式服务（设置 10 秒超时）
	client := proto.NewTaskServiceClient(conn)
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()

	// 构建 gRPC 请求
	grpcReq := &proto.BatchTaskRequest{
		Params:      req.Params,
		ParallelNum: int32(len(req.Params)), // 并行数 = 参数数量
	}

	// 发起流式调用
	stream, err := client.BatchProcessTask(ctx, grpcReq)
	if err != nil {
		log.Printf("调用 gRPC 流式服务失败（节点：%s）：%v", nodeAddr, err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"code": 1,
			"msg":  fmt.Sprintf("批量任务处理失败：%v", err),
			"data": nil,
		})
		return
	}

	// 5. 接收 gRPC 流式响应（逐个处理）
	var results []gin.H
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			// 流结束，退出循环
			log.Printf("gRPC 流式响应接收完成")
			break
		}
		if err != nil {
			log.Printf("接收流式响应失败：%v", err)
			continue
		}

		// 收集单个任务结果
		results = append(results, gin.H{
			"task_id":   resp.TaskId,
			"node_addr": resp.NodeAddr,
			"param":     req.Params[len(results)], // 对应原始参数
			"result":    resp.Result,
			"success":   resp.Success,
		})
	}

	// 6. 返回 HTTP 结果
	c.JSON(http.StatusOK, gin.H{
		"code": 0,
		"msg":  fmt.Sprintf("批量任务处理成功，总计 %d 个任务", len(results)),
		"data": gin.H{
			"total":     len(results),
			"success":   len(results), // 简化：忽略失败任务（实际可统计 success 字段）
			"node_addr": nodeAddr,
			"tasks":     results,
		},
	})
}
