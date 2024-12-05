package main

import (
	"C"
	"log"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
)

type Type struct {
	id    string
	Other string
}

// var compressionMap = map[string]CompressionMethod{
// 	"none":    CompressionNone,
// 	"zstd":    CompressionZSTD,
// 	"lz4":     CompressionLZ4,
// 	"gzip":    CompressionGZIP,
// 	"deflate": CompressionDeflate,
// 	"br":      CompressionBrotli,
// }

// switch params.Get(v) {
// case "in_order":
// 	o.ConnOpenStrategy = ConnOpenInOrder
// case "round_robin":
// 	o.ConnOpenStrategy = ConnOpenRoundRobin
// case "random":
// 	o.ConnOpenStrategy = ConnOpenRandom
// }

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	log.Printf("[multiinstance] Register called")
	return output.FLBPluginRegister(def, "multiinstance", "Testing multiple instances.")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	getConfig := func(key string, defaults ...string) string {
		value := output.FLBPluginConfigKey(plugin, key)
		if len(value) > 0 {
			return value
		}
		if len(defaults) > 0 {
			return defaults[0]
		}
		return ""
	}
	ctx, err := NewContext(getConfig)
	if err != nil {
		log.Println("init clickhouse plugin failed,", err)
		return output.FLB_ERROR
	}

	err = ctx.Init()
	if err != nil {
		log.Println("init clickhouse plugin failed,", err)
		return output.FLB_ERROR
	}
	// Set the context to point to any Go variable
	output.FLBPluginSetContext(plugin, ctx)

	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	log.Print("[multiinstance] Flush called for unknown instance")
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctxPtr, data unsafe.Pointer, length C.int, tag *C.char) int {
	// Type assert context back into the original type for the Go variable
	ctx := output.FLBPluginGetContext(ctxPtr).(*Context)

	// check connections
	err := ctx.Ping()
	if err != nil {
		log.Println("clickhouse plugin flush failed,", err)
		return output.FLB_RETRY
	}

	return ctx.BenchInsert(C.GoString(tag), output.NewDecoder(data, int(length)))
}

//export FLBPluginExit
func FLBPluginExit() int {
	log.Print("[multiinstance] Exit called for unknown instance")
	return output.FLB_OK
}

//export FLBPluginExitCtx
func FLBPluginExitCtx(ctxPtr unsafe.Pointer) int {
	// Type assert context back into the original type for the Go variable
	ctx := output.FLBPluginGetContext(ctxPtr).(*Context)
	ctx.Exit()
	return output.FLB_OK
}

//export FLBPluginUnregister
func FLBPluginUnregister(def unsafe.Pointer) {
	log.Print("[multiinstance] Unregister called")
	output.FLBPluginUnregister(def)
}

func main() {
}
