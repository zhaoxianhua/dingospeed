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

package service

import (
	"dingospeed/internal/dao"
	"dingospeed/pkg/consts"
	myerr "dingospeed/pkg/error"
	"dingospeed/pkg/util"

	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

type FileService struct {
	fileDao *dao.FileDao
}

func NewFileService(fileDao *dao.FileDao) *FileService {
	return &FileService{
		fileDao: fileDao,
	}
}

func (f *FileService) FileHeadCommon(c echo.Context, repoType, orgRepo, commit, filePath string) error {
	commitSha, err := f.fileDao.GetFileCommitSha(c, repoType, orgRepo, commit)
	if err != nil {
		if e, ok := err.(myerr.Error); ok {
			return util.ErrorEntryUnknown(c, e.StatusCode(), e.Error())
		}
		return util.ErrorProxyError(c)
	}
	return f.fileDao.FileGetGenerator(c, repoType, orgRepo, commitSha, filePath, consts.RequestTypeHead)
}

func (f *FileService) FileGetCommon(c echo.Context, repoType, orgRepo, commit, filePath string) error {
	zap.S().Infof("exec file get:%s/%s/%s/%s, remoteAdd:%s", repoType, orgRepo, commit, filePath, c.Request().RemoteAddr)
	commitSha, err := f.fileDao.GetFileCommitSha(c, repoType, orgRepo, commit)
	if err != nil {
		if e, ok := err.(myerr.Error); ok {
			return util.ErrorEntryUnknown(c, e.StatusCode(), e.Error())
		}
		return util.ErrorProxyError(c)
	}
	return f.fileDao.FileGetGenerator(c, repoType, orgRepo, commitSha, filePath, consts.RequestTypeGet)
}
