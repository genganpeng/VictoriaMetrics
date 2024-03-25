package stream

import (
	"bufio"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/prometheus"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/writeconcurrencylimiter"
)

// Parse parses lines with Prometheus exposition format from r and calls callback for the parsed rows.
//
// The callback can be called concurrently multiple times for streamed data from r.
//
// callback shouldn't hold rows after returning.
//
// limitConcurrency defines whether to control the number of concurrent calls to this function.
// It is recommended setting limitConcurrency=true if the caller doesn't have concurrency limits set,
// like /api/v1/write calls.
func Parse(r io.Reader, defaultTimestamp int64, isGzipped, limitConcurrency bool, callback func(rows []prometheus.Row) error, errLogger func(string)) error {
	// 优化点： 中型对象的分配和释放相对不是很频繁，因此使用sync.Pool来作为对象池，就可以重用这些对象。 中型对象提供了reset()方法，
	// 把缓冲区的开始位置置0，而不是解除引用。中型对象相关的所有成员都只会分配一次，然后不再释放。
	if limitConcurrency {
		wcr := writeconcurrencylimiter.GetReader(r)
		defer writeconcurrencylimiter.PutReader(wcr)
		r = wcr
	}

	if isGzipped {
		zr, err := common.GetGzipReader(r)
		if err != nil {
			return fmt.Errorf("cannot read gzipped lines with Prometheus exposition format: %w", err)
		}
		defer common.PutGzipReader(zr)
		r = zr
	}
	ctx := getStreamContext(r)
	defer putStreamContext(ctx)
	// 不断读取数据
	for ctx.Read() {
		uw := getUnmarshalWork()
		uw.errLogger = errLogger
		uw.ctx = ctx
		uw.callback = callback
		uw.defaultTimestamp = defaultTimestamp
		uw.reqBuf, ctx.reqBuf = ctx.reqBuf, uw.reqBuf
		ctx.wg.Add(1)
		// 通过channel异步处理反序列化， 参见StartUnmarshalWorkers启动异步处理工作
		common.ScheduleUnmarshalWork(uw)
		if wcr, ok := r.(*writeconcurrencylimiter.Reader); ok {
			wcr.DecConcurrency()
		}
	}
	ctx.wg.Wait()
	if err := ctx.Error(); err != nil {
		return err
	}
	return ctx.callbackErr
}

func (ctx *streamContext) Read() bool {
	readCalls.Inc()
	if ctx.err != nil || ctx.hasCallbackError() {
		return false
	}
	ctx.reqBuf, ctx.tailBuf, ctx.err = common.ReadLinesBlock(ctx.br, ctx.reqBuf, ctx.tailBuf)
	if ctx.err != nil {
		// EOF通过错误返回
		if ctx.err != io.EOF {
			readErrors.Inc()
			ctx.err = fmt.Errorf("cannot read Prometheus exposition data: %w", ctx.err)
		}
		return false
	}
	return true
}

type streamContext struct {
	br      *bufio.Reader
	reqBuf  []byte
	tailBuf []byte
	err     error

	wg              sync.WaitGroup
	callbackErrLock sync.Mutex
	callbackErr     error
}

func (ctx *streamContext) Error() error {
	if ctx.err == io.EOF {
		return nil
	}
	return ctx.err
}

func (ctx *streamContext) hasCallbackError() bool {
	ctx.callbackErrLock.Lock()
	ok := ctx.callbackErr != nil
	ctx.callbackErrLock.Unlock()
	return ok
}

func (ctx *streamContext) reset() {
	ctx.br.Reset(nil)
	ctx.reqBuf = ctx.reqBuf[:0]
	ctx.tailBuf = ctx.tailBuf[:0]
	ctx.err = nil
	ctx.callbackErr = nil
}

var (
	readCalls  = metrics.NewCounter(`vm_protoparser_read_calls_total{type="prometheus"}`)
	readErrors = metrics.NewCounter(`vm_protoparser_read_errors_total{type="prometheus"}`)
	rowsRead   = metrics.NewCounter(`vm_protoparser_rows_read_total{type="prometheus"}`)
)

func getStreamContext(r io.Reader) *streamContext {
	select {
	case ctx := <-streamContextPoolCh:
		ctx.br.Reset(r)
		return ctx
	default:
		if v := streamContextPool.Get(); v != nil {
			ctx := v.(*streamContext)
			ctx.br.Reset(r)
			return ctx
		}
		return &streamContext{
			br: bufio.NewReaderSize(r, 64*1024),
		}
	}
}

func putStreamContext(ctx *streamContext) {
	ctx.reset()
	select {
	case streamContextPoolCh <- ctx:
	default:
		streamContextPool.Put(ctx)
	}
}

var streamContextPool sync.Pool
var streamContextPoolCh = make(chan *streamContext, cgroup.AvailableCPUs())

// 负责反序列化的工作
type unmarshalWork struct {
	rows             prometheus.Rows
	ctx              *streamContext
	callback         func(rows []prometheus.Row) error
	errLogger        func(string)
	defaultTimestamp int64
	reqBuf           []byte
}

func (uw *unmarshalWork) reset() {
	uw.rows.Reset()
	uw.ctx = nil
	uw.callback = nil
	uw.errLogger = nil
	uw.defaultTimestamp = 0
	uw.reqBuf = uw.reqBuf[:0]
}

func (uw *unmarshalWork) runCallback(rows []prometheus.Row) {
	ctx := uw.ctx
	if err := uw.callback(rows); err != nil {
		ctx.callbackErrLock.Lock()
		if ctx.callbackErr == nil {
			ctx.callbackErr = fmt.Errorf("error when processing imported data: %w", err)
		}
		ctx.callbackErrLock.Unlock()
	}
	ctx.wg.Done()
}

// Unmarshal implements common.UnmarshalWork
func (uw *unmarshalWork) Unmarshal() {
	//
	if uw.errLogger != nil {
		uw.rows.UnmarshalWithErrLogger(bytesutil.ToUnsafeString(uw.reqBuf), uw.errLogger)
	} else {
		uw.rows.Unmarshal(bytesutil.ToUnsafeString(uw.reqBuf))
	}
	rows := uw.rows.Rows
	rowsRead.Add(len(rows))

	// Fill missing timestamps with the current timestamp.
	defaultTimestamp := uw.defaultTimestamp
	if defaultTimestamp <= 0 {
		defaultTimestamp = time.Now().UnixNano() / 1e6
	}
	for i := range rows {
		r := &rows[i]
		if r.Timestamp == 0 {
			r.Timestamp = defaultTimestamp
		}
	}

	uw.runCallback(rows)
	putUnmarshalWork(uw)
}

func getUnmarshalWork() *unmarshalWork {
	v := unmarshalWorkPool.Get()
	if v == nil {
		return &unmarshalWork{}
	}
	return v.(*unmarshalWork)
}

func putUnmarshalWork(uw *unmarshalWork) {
	uw.reset()
	unmarshalWorkPool.Put(uw)
}

var unmarshalWorkPool sync.Pool
