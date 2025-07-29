package service

import (
	"context"
	"time"

	"dingospeed/internal/dao"
	"dingospeed/internal/data"
	"dingospeed/pkg/config"
	"dingospeed/pkg/consts"
	"dingospeed/pkg/proto/manager"

	"go.uber.org/zap"
)

type SchedulerService struct {
	Client       manager.ManagerClient
	Ctx          context.Context
	schedulerDao *dao.SchedulerDao
}

func NewSchedulerService(schedulerDao *dao.SchedulerDao) *SchedulerService {
	return &SchedulerService{
		schedulerDao: schedulerDao,
	}
}

func (s *SchedulerService) Register() {
	s.schedulerDao.Client = s.Client
	response, err := s.schedulerDao.Register()
	if err != nil {
		return
	}
	config.SysConfig.Id = response.Id
	go s.heartbeat()
	go s.reportFileProcess()
}

func (s *SchedulerService) heartbeat() {
	ticker := time.NewTicker(time.Duration(config.SysConfig.Scheduler.HeartbeatPeriod) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := s.schedulerDao.Heartbeat()
			if err != nil {
				zap.S().Errorf("speed:%s connect err.%v", config.SysConfig.Scheduler.InstanceId, err)
				config.SysConfig.SetSchedulerModel(consts.SchedulerModeStandalone)
				break
			}
			if config.SysConfig.GetSchedulerModel() == consts.SchedulerModeStandalone {
				config.SysConfig.SetSchedulerModel(consts.SchedulerModeCluster)
			}
		case <-s.Ctx.Done():
			zap.S().Warnf("heartbeat stop.")
			return
		}
	}
}

func (s *SchedulerService) reportFileProcess() {
	for {
		select {
		case file, ok := <-data.GetFileProcessChan():
			if !ok {
				return
			}
			s.schedulerDao.ReportFileProcess(&manager.FileProcessRequest{
				ProcessId: file.ProcessId,
				StaPos:    file.StartPos,
				EndPos:    file.EndPos,
				Status:    file.Status,
			})
		case <-s.Ctx.Done():
			zap.S().Warnf("reportFileProcess stop.")
			return
		}
	}
}
