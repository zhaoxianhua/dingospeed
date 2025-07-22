package server

import (
	"context"

	"dingospeed/internal/service"
	"dingospeed/pkg/config"
	"dingospeed/pkg/proto/manager"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type SchedulerServer struct {
	conn             *grpc.ClientConn
	schedulerService *service.SchedulerService
}

func NewSchedulerServer(schedulerService *service.SchedulerService) *SchedulerServer {
	return &SchedulerServer{
		schedulerService: schedulerService,
	}
}

func (s *SchedulerServer) Start(ctx context.Context) error {
	if !config.SysConfig.IsCluster() {
		return nil
	}
	zap.S().Infof("enter cluster mode......")
	conn, err := grpc.NewClient(config.SysConfig.Scheduler.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		zap.S().Errorf("连接失败: %v", err)
		return err
	}
	s.conn = conn
	client := manager.NewManagerClient(conn)
	s.schedulerService.Client = client
	s.schedulerService.Ctx = ctx
	s.schedulerService.Register()
	return nil
}

func (s *SchedulerServer) Stop(ctx context.Context) error {
	if s.conn != nil {
		zap.S().Infof("[GRPC] client shutdown.")
		err := s.conn.Close()
		if err != nil {
			zap.S().Errorf("conn close fail.%v", err)
			return err
		}
	}
	return nil
}
