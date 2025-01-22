package loadgen

import (
	"context"
	"embed"
	"log"

	"github.com/mailru/easyjson"

	"github.com/vkcom/statshouse/internal/api"
)

//go:embed dashboard.json
var dashboardEmbed embed.FS

func EnsureDashboardExists(ctx context.Context, c *api.Client) error {
	data, _ := dashboardEmbed.ReadFile("dashboard.json")
	dashboardInfo := &api.DashboardInfo{}
	err := easyjson.Unmarshal(data, dashboardInfo)
	if err != nil {
		return err
	}
	id, err := dashboardExists(ctx, c, dashboardInfo.Dashboard.Name)
	if err != nil {
		return err
	}
	if id > 0 {
		log.Println("dashboard already exist will rewrite it")
		existingDashboard, err := c.GetDashboard(ctx, id)
		if err != nil {
			return err

		}
		dashboardInfo.Dashboard.DashboardID = existingDashboard.Dashboard.DashboardID
		dashboardInfo.Dashboard.Version = existingDashboard.Dashboard.Version
		err = c.PostDashboard(ctx, dashboardInfo)
		if err != nil {
			return err
		}
		log.Println("updated dashboard")
	} else {
		err = c.PutDashboard(ctx, dashboardInfo)
		if err != nil {
			return err
		}
		log.Println("created dashboard")
	}
	log.Printf("Dashboard: %s/view?id=%d\n", c.BaseURL(), dashboardInfo.Dashboard.DashboardID)
	return nil
}

func dashboardExists(ctx context.Context, c *api.Client, name string) (int32, error) {
	dashboards, err := c.ListDashboards(ctx)
	if err != nil {
		return -1, err
	}
	for _, d := range dashboards {
		if d.Name == name {
			return d.Id, nil
		}
	}
	return -1, nil
}
