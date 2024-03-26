package prometheusimport

import (
	"net/http"

	"github.com/VictoriaMetrics/metrics"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/netstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/relabel"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/auth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompbmarshal"
	parserCommon "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/common"
	parser "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/prometheus"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/prometheus/stream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/tenantmetrics"
)

var (
	rowsInserted       = metrics.NewCounter(`vm_rows_inserted_total{type="prometheus"}`)
	rowsTenantInserted = tenantmetrics.NewCounterMap(`vm_tenant_inserted_rows_total{type="prometheus"}`)
	rowsPerInsert      = metrics.NewHistogram(`vm_rows_per_insert{type="prometheus"}`)
)

// InsertHandler processes `/api/v1/import/prometheus` request.
func InsertHandler(at *auth.Token, req *http.Request) error {
	// 获取额外的标签
	extraLabels, err := parserCommon.GetExtraLabels(req)
	if err != nil {
		return err
	}
	defaultTimestamp, err := parserCommon.GetTimestamp(req)
	if err != nil {
		return err
	}
	isGzipped := req.Header.Get("Content-Encoding") == "gzip"
	return stream.Parse(req.Body, defaultTimestamp, isGzipped, true, func(rows []parser.Row) error {
		// 反序列化数据完成后执行插入操作
		return insertRows(at, rows, extraLabels)
	}, func(s string) {
		httpserver.LogError(req, s)
	})
}

func insertRows(at *auth.Token, rows []parser.Row, extraLabels []prompbmarshal.Label) error {
	ctx := netstorage.GetInsertCtx()
	defer netstorage.PutInsertCtx(ctx)

	ctx.Reset() // This line is required for initializing ctx internals.
	perTenantRows := make(map[auth.Token]int)
	hasRelabeling := relabel.HasRelabeling()
	// 一行一行数据处理
	for i := range rows {
		r := &rows[i]
		ctx.Labels = ctx.Labels[:0]
		// 指标名的label名为空字符串
		ctx.AddLabel("", r.Metric)
		// 获取数据中标签
		for j := range r.Tags {
			tag := &r.Tags[j]
			ctx.AddLabel(tag.Key, tag.Value)
		}
		// 获取请求中额外的标签
		for j := range extraLabels {
			label := &extraLabels[j]
			ctx.AddLabel(label.Name, label.Value)
		}
		// relabel
		if hasRelabeling {
			ctx.ApplyRelabeling()
		}
		if len(ctx.Labels) == 0 {
			// Skip metric without labels.
			continue
		}
		ctx.SortLabelsIfNeeded()
		// 如果at为nil，则获取标签中租户信息
		atLocal := ctx.GetLocalAuthToken(at)
		if err := ctx.WriteDataPoint(atLocal, ctx.Labels, r.Timestamp, r.Value); err != nil {
			return err
		}
		perTenantRows[*atLocal]++
	}
	rowsInserted.Add(len(rows))
	rowsTenantInserted.MultiAdd(perTenantRows)
	rowsPerInsert.Update(float64(len(rows)))
	return ctx.FlushBufs()
}
