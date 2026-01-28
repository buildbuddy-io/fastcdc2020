# fastcdc2020

A Go implementation of the FastCDC 2020 content-defined chunking algorithm.

Based on the paper ["FastCDC: A Fast and Efficient Content-Defined Chunking Approach for Data Deduplication"](https://ieeexplore.ieee.org/document/9055082) by Wen Xia, et al., with inspiration from [jotfs/fastcdc-go](https://github.com/jotfs/fastcdc-go).

## Usage

```go
package main

import (
	"bytes"
	"fmt"
	"io"

	"github.com/buildbuddy-io/fastcdc2020/fastcdc"
)

func main() {
	data := []byte("your data here...")

	averageChunkSizeBytes := 8192 // must be power of 2
	chunker, err := fastcdc.NewChunker(
		bytes.NewReader(data),
		averageChunkSizeBytes,
		fastcdc.WithMinSize(2048),
		fastcdc.WithMaxSize(32768),
	)
	if err != nil {
		panic(err)
	}

	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		fmt.Printf("offset=%d length=%d\n", chunk.Offset, chunk.Length)
	}
}
```

### Options

- `WithMinSize(size)` - Minimum chunk size (default: averageSize / 4)
- `WithMaxSize(size)` - Maximum chunk size (default: averageSize * 4)
- `WithNormalization(level)` - Normalization level 0-3 (default: 2, set to 0 to disable)
- `WithSeed(seed)` - Seed for gear hash to prevent fingerprinting attacks
- `WithBufferSize(size)` - Internal buffer size (default: maxSize * 2)

## Benchmarks

```
goos: linux
goarch: amd64
pkg: github.com/buildbuddy-io/fastcdc2020/fastcdc
cpu: AMD Ryzen 9 9950X3D 16-Core Processor
                │          B/s          │
Chunker/1k-32             19.47Gi ±  0%
Chunker/4k-32             62.00Gi ±  0%
Chunker/16k-32            135.4Gi ±  0%
Chunker/32k-32            98.46Gi ±  0%
Chunker/64k-32            109.6Gi ±  1%
Chunker/128k-32           116.5Gi ±  0%
Chunker/256k-32           151.0Gi ±  0%
Chunker/512k-32           6.003Gi ± 18%
Chunker/1M-32             3.963Gi ±  2%
Chunker/4M-32             3.828Gi ±  2%
Chunker/16M-32            4.191Gi ±  0%
Chunker/32M-32            4.238Gi ±  1%
Chunker/64M-32            4.092Gi ±  1%
Chunker/128M-32           3.996Gi ±  0%
Chunker/512M-32           4.082Gi ±  1%
Chunker/1G-32             4.012Gi ±  1%
```
