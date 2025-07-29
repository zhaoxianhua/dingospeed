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

package dao

import (
	"fmt"
	"net/http"
	"time"

	"dingospeed/pkg/common"
	"dingospeed/pkg/config"
	"dingospeed/pkg/util"
)

type MetaDao struct {
	fileDao *FileDao
}

func NewMetaDao(fileDao *FileDao) *MetaDao {
	return &MetaDao{
		fileDao: fileDao,
	}
}

func (m *MetaDao) RepoRefs(repoType string, orgRepo string, authorization string) (*common.Response, error) {
	refsUrl := fmt.Sprintf("%s/api/%s/%s/refs", config.SysConfig.GetHFURLBase(), repoType, orgRepo)
	headers := map[string]string{}
	if authorization != "" {
		headers["authorization"] = authorization
	}
	resp, err := util.RetryRequest(func() (*common.Response, error) {
		return util.Get(refsUrl, headers, config.SysConfig.GetReqTimeOut())
	})
	return resp, err
}

func (m *MetaDao) ForwardRefs(targetURL string, originalReq *http.Request, timeout time.Duration) (*common.Response, error) {
	resp, err := util.RetryRequest(func() (*common.Response, error) {
		return util.ForwardRequest(targetURL, originalReq, timeout)
	})
	return resp, err
}
