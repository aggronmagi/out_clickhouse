// Copyright [2024] [aggronmagi]
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

type ColumnParser func(string) (interface{}, error)

type ClickhouseContext struct {
	// clickhouse native link option
	Opt *clickhouse.Options

	// benchmark insert option
	Columns    []string
	Defaults   []any
	ColType    []ColumnParser
	RecordTime string
	TimeFormat string
	TimeStamp  int // 1:ns 2:us 3:ms 4:s
	Tags       []string
	TagType    []ColumnParser
	TagSplit   string
	TableName  string
	BenchStmt  string

	// running option
	Conn driver.Conn
}

// NewContext get clickhouse option
func NewContext(get func(key string, defaults ...string) string) (pctx *ClickhouseContext, err error) {
	pctx = &ClickhouseContext{
		Opt: &clickhouse.Options{},
	}
	opt := pctx.Opt
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
	pctx.TableName = tableName

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

	// cfg = get("Columns")
	// if cfg != "" {
	// }
	err = parseColumns(pctx, get("Columns"))
	if err != nil {
		return
	}

	cfg = strings.TrimSpace(get("RecordTime"))
	if cfg != "" {
		arr = strings.Split(cfg, ",")
		if len(arr) > 1 {
			pctx.RecordTime = arr[0]
			switch arr[1] {
			case "ns":
				pctx.TimeStamp = 1
			case "us":
				pctx.TimeStamp = 2
			case "ms":
				pctx.TimeStamp = 3
			case "s":
				pctx.TimeStamp = 4
			default:
				_, err = time.Parse(arr[1], arr[1])
				if err != nil {
					err = fmt.Errorf("invalid RecordTime value,timeFormat invalid. err:%w", err)
					return
				}
				pctx.TimeFormat = arr[1]
			}
		} else {
			pctx.RecordTime = cfg
		}
	}

	cfg = get("Tags")
	split := get("TagSplit", ".")
	if len(cfg) > 0 {
		arr = strings.Split(cfg, ",")
		for _, v := range arr {
			v = strings.TrimSpace(v)
			if v == "ignore" {
				v = ""
			}
			if strings.Contains(v, "|") {
				typInfo := strings.Split(v, "|")
				v = typInfo[0]
				parser, _, err := parseColumnType(typInfo[1:])
				if err != nil {
					err = fmt.Errorf("invalid Tags value[%s], parse type failed,%+v", v, err)
					return nil, err
				}
				pctx.TagType = append(pctx.TagType, parser)
			} else {
				pctx.TagType = append(pctx.TagType, parseString)
			}

			pctx.Tags = append(pctx.Tags, v)
			pctx.TagSplit = split
		}
	}
	return
}

func (pctx *ClickhouseContext) Init() (err error) {
	conn, err := clickhouse.Open(pctx.Opt)
	if err != nil {
		return fmt.Errorf("open clickhouse native connection failed,%w", err)
	}

	pctx.Conn = conn
	err = pctx.Ping()
	if err != nil {
		return
	}

	if len(pctx.Columns) > 0 {
		columns := make([]string, 0, len(pctx.Columns)+1+len(pctx.Tags))
		if len(pctx.RecordTime) > 0 {
			columns = append(columns, pctx.RecordTime)
		}
		for _, v := range pctx.Tags {
			if len(v) > 0 {
				columns = append(columns, v)
			}
		}
		columns = append(columns, pctx.Columns...)

		pctx.BenchStmt = "INSERT INTO " + pctx.Opt.Auth.Database + "." + pctx.TableName + "(" + strings.Join(columns, ",") + ")"
	}

	return nil
}

func (pctx *ClickhouseContext) Ping() error {
	cctx, cancel := context.WithTimeout(context.Background(), pctx.Opt.DialTimeout)
	defer cancel()
	err := pctx.Conn.Ping(cctx)
	if err != nil {
		return fmt.Errorf("ping clickhouse failed,%w", err)
	}
	return nil
}

func (pctx *ClickhouseContext) BenchInsert(tag string, dec *output.FLBDecoder) int {
	var tagValues []any
	if len(pctx.Tags) > 0 {
		values := strings.Split(tag, pctx.TagSplit)
		if len(values) != len(pctx.Tags) {
			log.Printf("clickhouse tag invalid, input:%+v need:%+v\n", values, pctx.Tags)
			return output.FLB_ERROR
		}
		for k := range pctx.Tags {
			if len(pctx.Tags[k]) > 0 {
				val, err := pctx.TagType[k](values[k])
				if err != nil {
					log.Printf("clickhouse tag parse failed, tag:%s value:%s err:%v\n", pctx.Tags[k], values[k], err)
					return output.FLB_ERROR
				}
				tagValues = append(tagValues, val)
			}
		}
	}

	// if len(pctx.Columns) == 0 {
	// 	return pctx.insertOneByOne(tagValues, dec)
	// }
	return pctx.insertBatch(tagValues, dec)
}

// func (pctx *ClickhouseContext) insertOneByOne(tagValues []any, dec *output.FLBDecoder) int {
// 	for {
// 		ret, ts, record := output.GetRecord(dec)
// 		if ret != 0 {
// 			break
// 		}

// 		columns := make([]any, 0, len(pctx.Columns)+len(tagValues)+1)
// 		if len(pctx.RecordTime) > 0 {
// 			var timestamp time.Time
// 			switch t := ts.(type) {
// 			case output.FLBTime:
// 				timestamp = ts.(output.FLBTime).Time
// 			case uint64:
// 				timestamp = time.Unix(int64(t), 0)
// 			default:
// 				fmt.Println("time provided invalid, defaulting to now.")
// 				timestamp = time.Now()
// 			}
// 			columns = append(columns, timestamp)
// 		}
// 		if len(tagValues) > 0 {
// 			columns = append(columns, tagValues...)
// 		}

// 		buf := strings.Builder{}
// 		buf.Grow(1024)
// 		buf.WriteString("INSERT INTO ")
// 		buf.WriteString(pctx.TableName)
// 		buf.WriteString("(")
// 		if len(pctx.RecordTime) > 0 {
// 			buf.WriteString(pctx.RecordTime)
// 			buf.WriteString(",")
// 		}
// 		if len(tagValues) > 0 {
// 			buf.WriteString(strings.Join(pctx.Tags, ","))
// 		}
// 		for k, v := range record {
// 			buf.WriteString(",")
// 			buf.WriteString(k.(string))
// 			columns = append(columns, v)
// 		}
// 		buf.WriteString(strings.Join(pctx.Columns, ","))
// 		buf.WriteString(") VALUES(")
// 		buf.WriteString(strings.Repeat("?,", len(columns)-1))
// 		buf.WriteString("?)")
// 		err := pctx.Conn.Exec(context.Background(), buf.String(), columns...)
// 		if err != nil {
// 			log.Printf("clickhouse insert failed, stmt:%s err:%v\n", buf.String(), err)
// 			return output.FLB_RETRY
// 		}
// 	}
// 	return output.FLB_OK
// }

func (pctx *ClickhouseContext) insertBatch(tagValues []any, dec *output.FLBDecoder) int {

	batch, err := pctx.Conn.PrepareBatch(context.Background(), pctx.BenchStmt)
	if err != nil {
		log.Printf("clickhouse prepare batch failed, stmt:%s err:%v\n", pctx.BenchStmt, err)
		return output.FLB_RETRY
	}

	for {
		ret, ts, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		columns := make([]any, 0, len(pctx.Columns)+len(tagValues)+1)
		if len(pctx.RecordTime) > 0 {
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
			columns = append(columns, timestamp)
		}
		if len(tagValues) > 0 {
			columns = append(columns, tagValues...)
		}
		for idx := range pctx.Columns {
			value, ok := record[pctx.Columns[idx]]
			if !ok {
				if pf, ok := pctx.Defaults[idx].(func() any); ok {
					columns = append(columns, pf())
					continue
				}
				columns = append(columns, pctx.Defaults[idx])
				continue
			}
			if str, ok := value.([]byte); ok {
				val, err := pctx.ColType[idx](unsafe.String(unsafe.SliceData(str), len(str)))
				if err != nil {
					log.Printf("clickhouse column parse failed, column:%s value:%s err:%v\n", pctx.Columns[idx], unsafe.String(unsafe.SliceData(str), len(str)), err)
					return output.FLB_ERROR
				}
				columns = append(columns, val)
			} else {
				columns = append(columns, value)
			}
		}
		err = batch.Append(columns...)
		if err != nil {
			log.Printf("clickhouse batch append failed,columns:[%#v] err:%v\n", columns, err)
			return output.FLB_RETRY
		}
	}
	err = batch.Send()
	if err != nil {
		log.Printf("clickhouse batch send failed, err:%v\n", err)
		return output.FLB_RETRY
	}
	return output.FLB_OK
}

func (pctx *ClickhouseContext) Exit() {

}
