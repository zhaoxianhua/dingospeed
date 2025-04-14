package downloader

import (
	"os"
	"sync"

	"dingo-hfmirror/pkg/common"
	"dingo-hfmirror/pkg/config"

	"go.uber.org/zap"
)

var (
	instance *DingCacheManager
	once     sync.Once
)

func GetInstance() *DingCacheManager {
	once.Do(func() {
		instance = &DingCacheManager{
			dingCacheMap: common.NewSafeMap[string, *DingCache](),
			dingCacheRef: common.NewSafeMap[string, int64](),
		}
	})
	return instance
}

type DingCacheManager struct {
	dingCacheMap *common.SafeMap[string, *DingCache]
	dingCacheRef *common.SafeMap[string, int64]
	mu           sync.RWMutex
}

func (f *DingCacheManager) GetDingFile(savePath string, fileSize int64) (*DingCache, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var dingFile *DingCache
	var ok bool
	if dingFile, ok = f.dingCacheMap.Get(savePath); ok {
		if refCount, ok := f.dingCacheRef.Get(savePath); ok {
			f.dingCacheRef.Set(savePath, refCount+1)
		} else {
			zap.S().Errorf("dingCacheRef key is not exist.key:%s", savePath)
		}
		return dingFile, nil
	} else {
		if _, err := os.Stat(savePath); err == nil {
			if dingFile, err = NewDingCache(savePath, config.SysConfig.Download.BlockSize); err != nil {
				zap.S().Errorf("NewDingCache err.%v", err)
				return nil, err
			}
		} else {
			if dingFile, err = NewDingCache(savePath, config.SysConfig.Download.BlockSize); err != nil {
				zap.S().Errorf("NewDingCache err.%v", err)
				return nil, err
			}
			if err = dingFile.Resize(fileSize); err != nil {
				zap.S().Errorf("Resize err.%v", err)
				return nil, err
			}
		}
		f.dingCacheMap.Set(savePath, dingFile)
		f.dingCacheRef.Set(savePath, 1)
	}
	return dingFile, nil
}

func (f *DingCacheManager) ReleasedDingFile(savePath string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	refCount, ok := f.dingCacheRef.Get(savePath)
	if !ok {
		return
	}
	refCount--
	zap.S().Debugf("ReleasedDingFile:%s, refcount:%d", savePath, refCount)
	if refCount <= 0 {
		if dingFile, ok := f.dingCacheMap.Get(savePath); ok {
			dingFile.Close()
		}
		f.dingCacheMap.Delete(savePath)
		f.dingCacheRef.Delete(savePath)
	} else {
		f.dingCacheRef.Set(savePath, refCount)
	}
}
