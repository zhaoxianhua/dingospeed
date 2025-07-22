//  Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http:www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package data

import (
	"context"

	"dingospeed/pkg/common"
	"dingospeed/pkg/config"
	"dingospeed/pkg/consts"

	"github.com/dgraph-io/ristretto/v2"
	"go.uber.org/zap"
)

// 缓存预读取的文件块，默认每个文件16个块

var FileBlockCache Cache[string, []byte]
var fileProcessChan chan common.FileProcess

func InitData() {
	if config.SysConfig.Cache.Enabled {
		if FileBlockCache == nil {
			// 默认使用ristretto
			if config.SysConfig.Cache.Type == 0 {
				cache, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
					NumCounters: 1e7,     // 计数器数量，用于预估缓存项的使用频率
					MaxCost:     1 << 30, // 缓存的最大成本，这里设置为 1GB
					BufferItems: 64,      // 每个分片的缓冲区大小
				})
				if err != nil {
					panic(err)
				}
				FileBlockCache = &RistrettoCache[string, []byte]{
					RCache: cache,
					cost:   1,
				}
			} else {
				FileBlockCache = common.NewSafeMap[string, []byte]()
			}
		}
	}
	if config.SysConfig.IsCluster() {
		fileProcessChan = make(chan common.FileProcess, 50)
	}
}

func GetFileProcessChan() <-chan common.FileProcess {
	return fileProcessChan
}

func ReportFileProcess(ctx context.Context, startPos, endPos int64, status int32) {
	if config.SysConfig.IsCluster() {
		if v := ctx.Value(consts.KeyProcessId); v != nil {
			processId := v.(int64)
			zap.S().Infof("processId:%d, startPos:%d, endPos:%d, status:%d", processId, startPos, endPos, status)
			fileProcessChan <- common.FileProcess{
				ProcessId: processId,
				StartPos:  startPos,
				EndPos:    endPos,
				Status:    status,
			}
		}
	}
}
