package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func main() {
	r := gin.Default()

	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})

	r.POST("/submit", func(c *gin.Context) {
		var requestData struct {
			Name  string `json:"name"`
			Email string `json:"email"`
		}
		if err := c.ShouldBindJSON(&requestData); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Invalid JSON format",
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"message": "Data received",
			"data": gin.H{
				"name":  requestData.Name,
				"email": requestData.Email,
			},
		})
	})

	err := r.Run(":8081")
	if err != nil {
		panic(err)
	}
}
