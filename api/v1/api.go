package v1

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/ticdc/api/middleware"
	pv2 "github.com/pingcap/ticdc/api/v2"
	"github.com/pingcap/ticdc/pkg/server"
)

func adjustStatusForV1(c *gin.Context) {
	c.Status(http.StatusAccepted)
	c.Next()
}

// OpenAPIV1 provides CDC v1 APIs
type OpenAPIV1 struct {
	server server.Server
	v2     pv2.OpenAPIV2
}

// NewOpenAPIV1 creates a new OpenAPIV1.
func NewOpenAPIV1(c server.Server) OpenAPIV1 {
	return OpenAPIV1{c, pv2.NewOpenAPIV2(c)}
}

// RegisterOpenAPIV1Routes registers routes for OpenAPIV1
func RegisterOpenAPIV1Routes(router *gin.Engine, api OpenAPIV1) {
	v1 := router.Group("/api/v1")

	v1.Use(middleware.LogMiddleware())
	v1.Use(middleware.ErrorHandleMiddleware())

	v1.GET("status", api.v2.ServerStatus)
	v1.POST("log", api.v2.SetLogLevel)

	coordinatorMiddleware := middleware.ForwardToCoordinatorMiddleware(api.server)
	authenticateMiddleware := middleware.AuthenticateMiddleware(api.server)
	v1.GET("health", coordinatorMiddleware, api.v2.ServerHealth)

	// changefeed API
	changefeedGroup := v1.Group("/changefeeds")
	changefeedGroup.GET("", coordinatorMiddleware, api.v2.ListChangeFeeds)
	changefeedGroup.GET("/:changefeed_id", coordinatorMiddleware, api.v2.GetChangeFeed)
	// These two APIs need to be adjusted to be compatible with the API v1.
	// changefeedGroup.POST("", coordinatorMiddleware, authenticateMiddleware, api.v2.CreateChangefeed)
	// changefeedGroup.PUT("/:changefeed_id", coordinatorMiddleware, authenticateMiddleware, api.v2.UpdateChangefeed)

	changefeedGroup.POST("/:changefeed_id/pause", coordinatorMiddleware, authenticateMiddleware, api.v2.PauseChangefeed, adjustStatusForV1)
	changefeedGroup.POST("/:changefeed_id/resume", coordinatorMiddleware, authenticateMiddleware, api.v2.ResumeChangefeed, adjustStatusForV1)
	changefeedGroup.DELETE("/:changefeed_id", coordinatorMiddleware, authenticateMiddleware, api.v2.DeleteChangefeed, adjustStatusForV1)
	// This API is not useful in new arch cdc, we implement it for compatibility with old arch cdc only.
	// changefeedGroup.POST("/:changefeed_id/tables/rebalance_table", coordinatorMiddleware, authenticateMiddleware, api.v2.RebalanceTables)
	// This API need to be adjusted to be compatible with the API v1.
	// changefeedGroup.POST("/:changefeed_id/tables/move_table", ownerMiddleware, authenticateMiddleware, api.MoveTable)

	// owner API
	ownerGroup := v1.Group("/owner")
	ownerGroup.POST("/resign", coordinatorMiddleware, api.v2.ResignOwner, adjustStatusForV1)

	// processor API
	processorGroup := v1.Group("/processors")
	processorGroup.GET("", coordinatorMiddleware, api.v2.ListProcessor)
	processorGroup.GET("/:changefeed_id/:capture_id",
		coordinatorMiddleware, api.v2.GetProcessor)

	// capture API
	captureGroup := v1.Group("/captures")
	captureGroup.Use(coordinatorMiddleware)
	captureGroup.GET("", api.v2.ListCaptures)

	// This API need to be adjusted to be compatible with the API v1.
	// captureGroup.PUT("/drain", api.v2.DrainCapture)
}
