all:
	go build -buildmode=c-shared -o out_clickhouse.so

clean:
	rm -rf *.so *.h *~
