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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Column = "name|type" | "name|DateTime64|2021-01-01 00:00:00.000000000"
func parseColumns(pctx *ClickhouseContext, cfg string) (err error) {
	arr := strings.Split(cfg, ",")
	pctx.Columns = make([]string, 0, len(arr))
	pctx.Defaults = make([]any, 0, len(arr))
	pctx.ColType = make([]ColumnParser, 0, len(arr))
	for _, v := range arr {
		v = strings.TrimSpace(v)
		if v == "" {
			err = fmt.Errorf("invalid Columns value[%s], has empty string", cfg)
			return
		}
		// pair := strings.Split(v, "=")
		// if len(pair) > 2 || len(pair) <= 0 {
		// 	err = fmt.Errorf("invalid Columns value[%s], need format KEY|TYPE[=Default]", v)
		// 	return
		// }
		// default value
		var def any
		typInfo := strings.Split(v, "|")
		if len(typInfo) < 2 {
			err = fmt.Errorf("invalid Columns value[%s], need format KEY|TYPE[=Default]", v)
			return
		}
		pctx.Columns = append(pctx.Columns, strings.TrimSpace(typInfo[0]))
		parser, def, err := parseColumnType(typInfo[1:])
		if err != nil {
			err = fmt.Errorf("invalid Columns value[%s], parse type failed,%+v", v, err)
			return err
		}
		pctx.ColType = append(pctx.ColType, parser)
		// // config default value
		// if len(pair) > 1 {
		// 	def, err = convertColumnDefaultValue(pair[1])
		// 	if err != nil {
		// 		err = fmt.Errorf("invalid Columns value[%s], parse default value failed,%+v", v, err)
		// 		return err
		// 	}
		// }

		pctx.Defaults = append(pctx.Defaults, def)
	}
	return
}

//UInt8, UInt16, UInt32, UInt64, UInt128, UInt256, Int8, Int16, Int32, Int64, Int128, Int256
// Float32 和 Float64) 和 Decimal
//  Boolean
// String 和 FixedString
// 使用 Date 和 Date32 表示天，使用 DateTime 和 DateTime64 表示时间点
// JSON 对象 在单个列中存储 JSON 文档
// 用于存储 UUID 值 的高效选项
// 当您只有少量唯一值时，使用 Enum；当您有一列最多 10,000 个唯一值时，使用 LowCardinality
// 使用 Map 存储键值对
// *数组**: 任何列都可以定义为 值的数组
// **IP 地址**: 使用 IPv4 和 IPv6 高效地存储 IP 地址
// **地理类型**: 用于 地理数据，包括 Point、Ring、Polygon 和 MultiPolygon
// **特殊数据类型**: 包括 Expression、Set、Nothing 和 Interval

func parseColumnType(cfg []string) (parser ColumnParser, def any, err error) {
	switch strings.ToLower(strings.TrimSpace(cfg[0])) {
	case "uint8":
		parser = parseUint[uint8]
		def = uint8(0)
	case "uint16":
		parser = parseUint[uint16]
		def = uint16(0)
	case "uint32":
		parser = parseUint[uint32]
		def = uint32(0)
	case "uint64":
		parser = parseUint[uint64]
		def = uint64(0)
	case "int8":
		parser = parseInt[int8]
		def = int8(0)
	case "int16":
		parser = parseInt[int16]
		def = int16(0)
	case "int32":
		parser = parseInt[int32]
		def = int32(0)
	case "int64":
		parser = parseInt[int64]
		def = int64(0)
	case "float32":
		parser = parseFloat[float32]
		def = float32(0)
	case "float64":
		parser = parseFloat[float64]
		def = float64(0)
	case "boolean":
		parser = parseBool
		def = false
	case "string":
		parser = parseString
		def = ""
	case "date", "date32":
		timeFormat := "2006-01-02"
		if len(cfg) > 1 {
			_, err = time.Parse(cfg[1], cfg[1])
			if err != nil {
				err = fmt.Errorf("invalid Columns value[%s], parse time format failed,%+v", cfg, err)
				return
			}
			timeFormat = cfg[1]
		}
		parser = func(s string) (interface{}, error) {
			return time.Parse(timeFormat, s)
		}
		def = func() any { return time.Now() }
	case "datetime":
		timeFormat := "2006-01-02 15:04:05"
		if len(cfg) > 1 {
			_, err = time.Parse(cfg[1], cfg[1])
			if err != nil {
				err = fmt.Errorf("invalid Columns value[%s], parse time format failed,%+v", cfg, err)
				return
			}
			timeFormat = cfg[1]
		}
		parser = func(s string) (interface{}, error) {
			return time.Parse(timeFormat, s)
		}
		def = func() any { return time.Now() }
	case "datetime64":
		timeFormat := "2006-01-02 15:04:05.000000000"
		if len(cfg) > 1 {
			_, err = time.Parse(cfg[1], cfg[1])
			if err != nil {
				err = fmt.Errorf("invalid Columns value[%s], parse time format failed,%+v", cfg, err)
				return
			}
			timeFormat = cfg[1]
		}
		parser = func(s string) (interface{}, error) {
			return time.Parse(timeFormat, s)
		}
		def = func() any { return time.Now() }
	default:
		err = fmt.Errorf("invalid Columns value[%s], not support type", cfg)
	}
	return
}

func parseInt[T int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64](s string) (any, error) {
	nv, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return T(nv), nil
}

func parseUint[T uint | uint8 | uint16 | uint32 | uint64](s string) (any, error) {
	nv, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return T(nv), nil
}

func parseFloat[T float32 | float64](s string) (any, error) {
	nv, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	return T(nv), nil
}

func parseBool(s string) (any, error) {
	nv, err := strconv.ParseBool(s)
	if err != nil {
		return false, err
	}
	return nv, nil
}

func parseString(s string) (any, error) {
	return s, nil
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
