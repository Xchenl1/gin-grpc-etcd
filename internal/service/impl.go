package service

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"gin+grpc+etcd/pkg/proto/api"
)

// TaskServiceImpl gRPC 服务实现类
type TaskServiceImpl struct {
	proto.UnimplementedTaskServiceServer        // 必须嵌入（兼容 gRPC 版本）
	nodeAddr                             string // 当前节点地址（如 127.0.0.1:50051）
}

// NewTaskServiceImpl 创建服务实例
func NewTaskServiceImpl(nodeAddr string) *TaskServiceImpl {
	return &TaskServiceImpl{
		nodeAddr: nodeAddr,
	}
}

// ProcessTask 处理单个任务（Unary RPC）
func (s *TaskServiceImpl) ProcessTask(ctx context.Context, req *proto.TaskRequest) (*proto.TaskResponse, error) {
	log.Printf("节点 %s 接收任务：taskID=%s, param=%s", s.nodeAddr, req.TaskId, req.Param)

	// 模拟业务处理（100-300 毫秒）
	time.Sleep(time.Duration(rand.Intn(200)+100) * time.Millisecond)

	// 业务逻辑示例：参数大写处理
	result := "processed-" + strings.ToUpper(req.Param)

	return &proto.TaskResponse{
		TaskId:   req.TaskId,
		Success:  true,
		Result:   result,
		ErrorMsg: "",
		NodeAddr: s.nodeAddr,
	}, nil
}

// BatchProcessTask 批量处理任务（Server Streaming）
func (s *TaskServiceImpl) BatchProcessTask(req *proto.BatchTaskRequest, stream proto.TaskService_BatchProcessTaskServer) error {
	log.Printf("节点 %s 接收批量任务：参数数=%d, 并行数=%d", s.nodeAddr, len(req.Params), req.ParallelNum)

	// 模拟批量处理（逐个返回结果）
	for i, param := range req.Params {
		taskID := fmt.Sprintf("batch-task-%d-%d", time.Now().UnixMilli(), i)
		time.Sleep(100 * time.Millisecond) // 模拟处理耗时

		// 向客户端流式发送结果
		if err := stream.Send(&proto.TaskResponse{
			TaskId:   taskID,
			Success:  true,
			Result:   "batch-processed-" + strings.ToUpper(param),
			ErrorMsg: "",
			NodeAddr: s.nodeAddr,
		}); err != nil {
			log.Printf("流式发送结果失败：%v", err)
			return err
		}
	}

	return nil
}
