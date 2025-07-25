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

package handler

import (
	"fmt"
	"strings"

	"dingospeed/internal/service"
	"dingospeed/pkg/consts"

	"github.com/labstack/echo/v4"
)

type MetaHandler struct {
	metaService *service.MetaService
}

func NewMetaHandler(fileService *service.MetaService) *MetaHandler {
	return &MetaHandler{
		metaService: fileService,
	}
}

func (handler *MetaHandler) MetaProxyCommonHandler(c echo.Context) error {
	repoType := c.Param("repoType")
	org := c.Param("org")
	repo := c.Param("repo")
	commit := c.Param("commit")
	method := strings.ToLower(c.Request().Method)
	orgRepo := fmt.Sprintf("%s/%s", org, repo)
	c.Set(consts.PromOrgRepo, orgRepo)
	return handler.metaService.MetaProxyCommon(c, repoType, org, repo, commit, method)
}

func (handler *MetaHandler) WhoamiV2Handler(c echo.Context) error {
	return handler.metaService.WhoamiV2(c)
}

func (handler *MetaHandler) ReposHandler(c echo.Context) error {
	return handler.metaService.Repos(c)
}

func (handler *MetaHandler) RepoRefsHandler(c echo.Context) error {
	repoType := c.Param("repoType")
	org := c.Param("org")
	repo := c.Param("repo")
	return handler.metaService.RepoRefs(c, repoType, org, repo)
}

func (handler *MetaHandler) ForwardToNewSiteHandler(c echo.Context) error {
	return handler.metaService.ForwardToNewSite(c)
}
