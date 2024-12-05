package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/fluent/fluent-bit-go/output"
)

type Context struct {
	// clickhouse native link option
	Opt *clickhouse.Options

	// benchmark insert option
	Columns    []string
	Defaults   []any
	RecordTime string
	Tags       []string
	TagSplit   string
	BenchStmt  string

	// running option
	Conn driver.Conn
}

// NewContext get clickhouse option
func NewContext(get func(key string, defaults ...string) string) (ctx *Context, err error) {
	ctx = &Context{
		Opt: &clickhouse.Options{},
	}
	opt := ctx.Opt
	if val := get("TCP"); val != "" {
		opt.Addr = strings.Split(val, ",")
	} else if val = get("HTTP"); val != "" {
		opt.Addr = strings.Split(val, ",")
		opt.Protocol = clickhouse.HTTP
	} else {
		err = errors.New("not set TCP or HTTP")
		return
	}
	if val := get("DB"); val != "" {
		opt.Auth.Database = val
	} else {
		err = errors.New("not set DB")
		return
	}
	if val := get("UserName"); val != "" {
		opt.Auth.Username = val
	} else {
		err = errors.New("not set UserName")
		return
	}
	if val := get("Password"); val != "" {
		opt.Auth.Password = val
	}
	tableName := get("Table")
	if tableName == "" {
		err = errors.New("not set Table")
		return
	}

	if val := get("Debug"); val != "" {
		ok, err := strconv.ParseBool(val)
		if err != nil {
			return nil, fmt.Errorf("invalid Debug value,%w", err)
		}
		opt.Debug = ok
		if ok {
			opt.Debugf = log.Printf
		}
	}

	cfg := get("MaxOpenConns", "5")
	nv, err := strconv.Atoi(cfg)
	if err != nil {
		err = fmt.Errorf("invalid MaxOpenConns value,%w", err)
		return
	}
	opt.MaxOpenConns = nv

	cfg = get("MaxIdleConns", "5")
	nv, err = strconv.Atoi(cfg)
	if err != nil {
		err = fmt.Errorf("invalid MaxIdleConns value,%w", err)
		return
	}
	opt.MaxIdleConns = nv

	cfg = get("DialTimeout", "30s")
	dv, err := time.ParseDuration(cfg)
	if err != nil {
		err = fmt.Errorf("invalid DialTimeout value, %w", err)
		return
	}
	opt.DialTimeout = dv

	cfg = get("ConnMaxLifetime", "10m")
	dv, err = time.ParseDuration(cfg)
	if err != nil {
		err = fmt.Errorf("invalid ConnMaxLifetime value, %w", err)
		return
	}
	opt.ConnMaxLifetime = dv

	cfg = get("ConnOpenStrategy")
	switch cfg {
	case "in_order":
		opt.ConnOpenStrategy = clickhouse.ConnOpenInOrder
	case "round_robin":
		opt.ConnOpenStrategy = clickhouse.ConnOpenRoundRobin
	case "random":
		opt.ConnOpenStrategy = clickhouse.ConnOpenRandom
	default:
		err = fmt.Errorf("invalid ConnOpenStrategy value[%v], need in {in_order round_robin random}", cfg)
		return
	}
	//none lz4 zstd gzip deflate br
	cfg = get("Compression")
	switch cfg {
	case "", "none":
	case "lz4":
		opt.Compression = &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		}
	case "gzip":
		opt.Compression = &clickhouse.Compression{
			Method: clickhouse.CompressionGZIP,
		}
	case "deflate":
		opt.Compression = &clickhouse.Compression{
			Method: clickhouse.CompressionDeflate,
		}
	case "zstd":
		opt.Compression = &clickhouse.Compression{
			Method: clickhouse.CompressionZSTD,
			Level:  3,
		}
		cfg = get("CompressionLevel")
		if cfg != "" {
			nv, err = strconv.Atoi(cfg)
			if err != nil {
				err = fmt.Errorf("invalid CompressionLevel value, %w", err)
				return
			}
			opt.Compression.Level = nv
		}
	case "br":
		opt.Compression = &clickhouse.Compression{
			Method: clickhouse.CompressionBrotli,
			Level:  3,
		}
		cfg = get("CompressionLevel")
		if cfg != "" {
			nv, err = strconv.Atoi(cfg)
			if err != nil {
				err = fmt.Errorf("invalid CompressionLevel value, %w", err)
				return
			}
			opt.Compression.Level = nv
		}
	}

	cfg = get("BlockBufferSize", "10")
	uv, err := strconv.ParseUint(cfg, 10, 8)
	if err != nil {
		err = fmt.Errorf("invalid BlockBufferSize value,%w", err)
		return
	}
	opt.BlockBufferSize = uint8(uv)

	cfg = get("MaxCompressionBuffer", "10240")
	nv, err = strconv.Atoi(cfg)
	if err != nil {
		err = fmt.Errorf("invalid MaxCompressionBuffer value,%w", err)
		return
	}
	opt.MaxCompressionBuffer = nv

	cfg = get("ClientInfo")
	arr := strings.Split(cfg, ",")
	for _, v := range arr {
		infos := strings.Split(strings.TrimSpace(v), "/")
		if len(infos) != 2 {
			err = fmt.Errorf("invalid ClientInfo value[%s], split '/' count != 2 ", err)
			return
		}
		opt.ClientInfo.Products = append(opt.ClientInfo.Products, struct {
			Name    string
			Version string
		}{
			Name:    strings.TrimSpace(infos[0]),
			Version: strings.TrimSpace(infos[1]),
		})
	}

	cfg = get("Columns")
	if cfg == "" {
		err = fmt.Errorf("invalid Columns value,empty")
		return
	}
	arr = strings.Split(cfg, ",")
	ctx.Columns = make([]string, 0, len(arr))
	for _, v := range arr {
		v = strings.TrimSpace(v)
		if v == "" {
			err = fmt.Errorf("invalid Columns value[%s], has empty string", cfg)
			return
		}
		pair := strings.Split(v, "=")
		if len(pair) != 2 {
			err = fmt.Errorf("invalid Columns value[%s], need format KEY=Default", v)
			return
		}
		// default value
		var def any
		def, err = convertColumnDefaultValue(pair[1])
		if err != nil {
			err = fmt.Errorf("invalid Columns value[%s], parse default value failed,%+v", v, err)
			return
		}
		ctx.Columns = append(ctx.Columns, strings.TrimSpace(pair[0]))
		ctx.Defaults = append(ctx.Defaults, def)
	}

	cfg = strings.TrimSpace(get("RecordTime"))
	if cfg != "" {
		ctx.RecordTime = cfg
	}

	cfg = get("Tags")
	split := get("TagSplit", ",")
	arr = strings.Split(cfg, ".")
	for _, v := range arr {
		v = strings.TrimSpace(v)
		if v == "ignore" {
			v = ""
		}
		ctx.Tags = append(ctx.Tags, v)
		ctx.TagSplit = split
	}

	columns := make([]string, 0, len(ctx.Columns)+1+len(ctx.Tags))
	if len(ctx.RecordTime) > 0 {
		columns = append(columns, ctx.RecordTime)
	}
	for _, v := range ctx.Tags {
		if len(v) > 0 {
			columns = append(columns, v)
		}
	}
	columns = append(columns, ctx.Columns...)

	ctx.BenchStmt = "INSERT INTO " + tableName + "(" + strings.Join(columns, ",") + ")"

	return
}

func (ctx *Context) Init() (err error) {
	conn, err := clickhouse.Open(ctx.Opt)
	if err != nil {
		return fmt.Errorf("open clickhouse native connection failed,%w", err)
	}

	ctx.Conn = conn
	return ctx.Ping()
}

func (ctx *Context) Ping() error {
	cctx, cancel := context.WithTimeout(context.Background(), ctx.Opt.DialTimeout)
	defer cancel()
	err := ctx.Conn.Ping(cctx)
	if err != nil {
		return fmt.Errorf("ping clickhouse failed,%w", err)
	}
	return nil
}

func (ctx *Context) BenchInsert(tag string, dec *output.FLBDecoder) int {

	var tagValues []any
	if len(ctx.TagSplit) > 0 {
		values := strings.Split(tag, ctx.TagSplit)
		if len(values) != len(ctx.Tags) {
			log.Printf("clickhouse tag invalid, input:%+v need:%+v\n", values, ctx.Tags)
			return output.FLB_ERROR
		}
		for k := range ctx.Tags {
			if len(ctx.Tags[k]) > 0 {
				tagValues = append(tagValues, values[k])
			}
		}
	}

	batch, err := ctx.Conn.PrepareBatch(context.Background(), ctx.BenchStmt)
	if err != nil {
		log.Printf("clickhouse prepare batch failed, stmt:%s err:%v\n", ctx.BenchStmt, err)
		return output.FLB_RETRY
	}

	for {
		ret, ts, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		columns := make([]any, 0, len(ctx.Columns)+len(tagValues)+1)
		if len(ctx.RecordTime) > 0 {
			var timestamp time.Time
			switch t := ts.(type) {
			case output.FLBTime:
				timestamp = ts.(output.FLBTime).Time
			case uint64:
				timestamp = time.Unix(int64(t), 0)
			default:
				fmt.Println("time provided invalid, defaulting to now.")
				timestamp = time.Now()
			}
			columns = append(columns, timestamp.UTC().Format(time.RFC3339Nano))
		}
		if len(tagValues) > 0 {
			columns = append(columns, tagValues...)
		}
		for idx := range ctx.Columns {
			value, ok := record[ctx.Columns[idx]]
			if !ok {
				columns = append(columns, ctx.Defaults[idx])
				continue
			}
			if str, ok := value.([]byte); ok {
				columns = append(columns, unsafe.String(unsafe.SliceData(str), len(str)))
			} else {
				columns = append(columns, value)
			}
		}
		err = batch.Append(columns...)
		if err != nil {
			return output.FLB_RETRY
		}
	}
	err = batch.Send()
	if err != nil {
		return output.FLB_RETRY
	}
	return output.FLB_OK
}

func (ctx *Context) Exit() {

}

func convertColumnDefaultValue(in string) (val any, err error) {
	in = strings.TrimSpace(in)
	// string
	if len(in) == 0 || in == `""` || in == `''` {
		val = ""
		return
	}
	if strings.HasPrefix(in, `"`) {
		if !strings.HasSuffix(in, `"`) {
			err = errors.New(`invalid string value, " not match `)
			return
		}
		val = in[1 : len(in)-1]
		return
	}
	if strings.HasPrefix(in, `'`) {
		if !strings.HasSuffix(in, `'`) {
			err = errors.New(`invalid string value, ' not match `)
			return
		}
		val = in[1 : len(in)-1]
		return
	}
	// float
	if strings.HasSuffix(in, `f`) {
		var fv float64
		fv, err = strconv.ParseFloat(in[:len(in)-1], 64)
		if err != nil {
			err = errors.New(`invalid string value, ' not match `)
			return
		}
		val = fv
		return
	}
	// bool
	switch strings.ToLower(in) {
	case "true", "on", "t", "y", "yes":
		val = true
		return
	case "false", "off", "f", "n", "no":
		val = false
		return
	}
	// unsigned
	if strings.HasSuffix(in, `u`) {
		var uv uint64
		uv, err = strconv.ParseUint(in[:len(in)-1], 10, 64)
		if err != nil {
			err = errors.New(`invalid string value, ' not match `)
			return
		}
		val = uv
		return
	}
	// signed
	nv, err := strconv.ParseInt(in, 10, 64)
	if err != nil {
		return
	}

	val = nv

	return
}
