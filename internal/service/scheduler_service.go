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
		runModeChange(consts.SchedulerModeStandalone)
		return
	}
	zap.S().Infof("enter cluster mode......")
	config.SysConfig.Id = response.Id
	go s.Heartbeat()
	go s.ReportFileProcess()
}

func (s *SchedulerService) Heartbeat() {
	ticker := time.NewTicker(time.Duration(config.SysConfig.Scheduler.Discovery.HeartbeatPeriod) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := s.schedulerDao.Heartbeat()
			if err != nil {
				zap.S().Errorf("speed:%s connect err.%v", config.SysConfig.Scheduler.Discovery.InstanceId, err)
				runModeChange(consts.SchedulerModeStandalone)
				break
			}
			runModeChange(consts.SchedulerModeCluster)
		case <-s.Ctx.Done():
			zap.S().Warnf("Heartbeat stop.")
			return
		}
	}
}

func (s *SchedulerService) ReportFileProcess() {
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
			zap.S().Warnf("ReportFileProcess stop.")
			return
		}
	}
}

func runModeChange(mode string) {
	if mode == consts.SchedulerModeStandalone {
		if config.SysConfig.GetSchedulerModel() == consts.SchedulerModeCluster {
			zap.S().Warnf("changed to standalone mode......")
			config.SysConfig.SetSchedulerModel(consts.SchedulerModeStandalone)
		}
	} else if mode == consts.SchedulerModeCluster {
		if config.SysConfig.GetSchedulerModel() == consts.SchedulerModeStandalone {
			zap.S().Warnf("changed to cluster mode......")
			config.SysConfig.SetSchedulerModel(consts.SchedulerModeCluster)
		}
	}

}
