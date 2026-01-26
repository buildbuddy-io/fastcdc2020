package fastcdc

import (
	"bytes"
	"io"
	"math/rand"
	"os"
	"testing"
)

// Expected values from https://github.com/nlfiedler/fastcdc-rs/blob/master/src/v2020/mod.rs#L903
func TestChunker_SekienAkashita(t *testing.T) {
	data, err := os.ReadFile("testdata/SekienAkashita.jpg")
	if err != nil {
		t.Skipf("test file not found: %v", err)
	}

	chunker, err := NewChunker(bytes.NewReader(data), 16384,
		WithMinSize(4096),
		WithMaxSize(65535),
		WithNormalization(1),
	)
	if err != nil {
		t.Fatalf("failed to create chunker: %v", err)
	}

	expected := []struct {
		hash   uint64
		length int
	}{
		{17968276318003433923, 21325},
		{8197189939299398838, 17140},
		{13019990849178155730, 28084},
		{4509236223063678303, 18217},
		{2504464741100432583, 24700},
	}

	var chunks []struct {
		hash   uint64
		length int
	}

	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("error reading chunk: %v", err)
		}
		chunks = append(chunks, struct {
			hash   uint64
			length int
		}{chunk.Fingerprint, chunk.Length})
	}

	if len(chunks) != len(expected) {
		t.Errorf("expected %d chunks, got %d", len(expected), len(chunks))
		for i, c := range chunks {
			t.Logf("  %d: length=%d hash=%d", i, c.length, c.hash)
		}
		return
	}

	for i, e := range expected {
		if chunks[i].length != e.length {
			t.Errorf("chunk %d: expected length %d, got %d", i, e.length, chunks[i].length)
		}
		if chunks[i].hash != e.hash {
			t.Errorf("chunk %d: expected hash %d, got %d", i, e.hash, chunks[i].hash)
		}
	}
}

// Expected values from https://github.com/nlfiedler/fastcdc-rs/blob/master/src/v2020/mod.rs#L928
func TestChunker_SekienWithSeed(t *testing.T) {
	data, err := os.ReadFile("testdata/SekienAkashita.jpg")
	if err != nil {
		t.Skipf("test file not found: %v", err)
	}

	chunker, err := NewChunker(bytes.NewReader(data), 16384,
		WithMinSize(4096),
		WithMaxSize(65535),
		WithNormalization(1),
		WithSeed(666),
	)
	if err != nil {
		t.Fatalf("failed to create chunker: %v", err)
	}

	expected := []struct {
		hash   uint64
		length int
	}{
		{9312357714466240148, 10605},
		{226910853333574584, 55745},
		{12271755243986371352, 11346},
		{14153975939352546047, 5883},
		{5890158701071314778, 11586},
		{8981594897574481255, 14301},
	}

	var chunks []struct {
		hash   uint64
		length int
	}

	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("error reading chunk: %v", err)
		}
		chunks = append(chunks, struct {
			hash   uint64
			length int
		}{chunk.Fingerprint, chunk.Length})
	}

	if len(chunks) != len(expected) {
		t.Errorf("expected %d chunks, got %d", len(expected), len(chunks))
		for i, c := range chunks {
			t.Logf("  %d: length=%d hash=%d", i, c.length, c.hash)
		}
		return
	}

	for i, e := range expected {
		if chunks[i].length != e.length {
			t.Errorf("chunk %d: expected length %d, got %d", i, e.length, chunks[i].length)
		}
		if chunks[i].hash != e.hash {
			t.Errorf("chunk %d: expected hash %d, got %d", i, e.hash, chunks[i].hash)
		}
	}
}

func TestChunker_BasicChunking(t *testing.T) {
	data := randBytes(1e6, 63)
	chunker, err := NewChunker(bytes.NewReader(data), 1024)
	if err != nil {
		t.Fatal(err)
	}

	var prevOffset int
	var prevLength int
	allData := make([]byte, 0)
	for i := 0; ; i++ {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}

		offset := prevOffset + prevLength
		if offset != chunk.Offset {
			t.Errorf("chunk %d: Offset should be %d not %d", i, offset, chunk.Offset)
		}
		if chunk.Length != len(chunk.Data) {
			t.Errorf("chunk %d: Length %d does not match len(Data) %d", i, chunk.Length, len(chunk.Data))
		}

		allData = append(allData, chunk.Data...)

		prevOffset = chunk.Offset
		prevLength = chunk.Length
	}
	if !bytes.Equal(allData, data) {
		t.Error("reconstructed data does not match original")
	}
}

func TestChunker_Deterministic(t *testing.T) {
	data := randBytes(100000, 42)

	getChunks := func() []int {
		chunker, err := NewChunker(bytes.NewReader(data), 4096)
		if err != nil {
			t.Fatal(err)
		}

		var lengths []int
		for {
			chunk, err := chunker.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			lengths = append(lengths, chunk.Length)
		}
		return lengths
	}

	first := getChunks()
	second := getChunks()

	if len(first) != len(second) {
		t.Fatalf("chunk count differs: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Errorf("chunk %d length differs: %d vs %d", i, first[i], second[i])
		}
	}
}

func TestChunker_Options(t *testing.T) {
	tests := []struct {
		name        string
		averageSize int
		opts        []Option
		wantErr     bool
	}{
		{
			name:        "invalid average size",
			averageSize: 0,
			opts:        []Option{},
			wantErr:     true,
		},
		{
			name:        "basic valid options",
			averageSize: 8192,
			opts:        []Option{},
			wantErr:     false,
		},
		{
			name:        "all options specified",
			averageSize: 8192,
			opts: []Option{
				WithMinSize(2048),
				WithMaxSize(32768),
				WithNormalization(1),
				WithBufferSize(65536),
			},
			wantErr: false,
		},
		{
			name:        "without normalization",
			averageSize: 8192,
			opts: []Option{
				WithNormalization(0),
			},
			wantErr: false,
		},
		{
			name:        "with seed",
			averageSize: 8192,
			opts: []Option{
				WithSeed(666), // Must match TestChunker_SekienWithSeed
			},
			wantErr: false,
		},
		{
			name:        "min greater than max",
			averageSize: 8192,
			opts: []Option{
				WithMinSize(10000),
				WithMaxSize(5000),
			},
			wantErr: true,
		},
		{
			name:        "average outside range",
			averageSize: 8192,
			opts: []Option{
				WithMinSize(1024),
				WithMaxSize(4096),
			},
			wantErr: true,
		},
		{
			name:        "invalid normalization",
			averageSize: 8192,
			opts: []Option{
				WithNormalization(5),
			},
			wantErr: true,
		},
	}

	data := randBytes(10000, 99)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewChunker(bytes.NewReader(data), tt.averageSize, tt.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewChunker() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestChunker_Reset(t *testing.T) {
	data := randBytes(50000, 77)

	chunker, err := NewChunker(bytes.NewReader(data), 4096)
	if err != nil {
		t.Fatal(err)
	}

	var firstLengths []int
	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		firstLengths = append(firstLengths, chunk.Length)
	}

	chunker.Reset(bytes.NewReader(data))
	var secondLengths []int
	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		secondLengths = append(secondLengths, chunk.Length)
	}

	if len(firstLengths) != len(secondLengths) {
		t.Fatalf("chunk count differs after reset: %d vs %d", len(firstLengths), len(secondLengths))
	}
	for i := range firstLengths {
		if firstLengths[i] != secondLengths[i] {
			t.Errorf("chunk %d length differs after reset: %d vs %d", i, firstLengths[i], secondLengths[i])
		}
	}
}

func TestChunker_EdgeCases(t *testing.T) {
	t.Run("empty input", func(t *testing.T) {
		chunker, err := NewChunker(bytes.NewReader(nil), 1024)
		if err != nil {
			t.Fatal(err)
		}

		_, err = chunker.Next()
		if err != io.EOF {
			t.Errorf("expected io.EOF for empty input, got %v", err)
		}
	})

	t.Run("small input", func(t *testing.T) {
		data := randBytes(10, 51)
		chunker, err := NewChunker(bytes.NewReader(data), 1024,
			WithNormalization(0),
		)
		if err != nil {
			t.Fatal(err)
		}

		chunk, err := chunker.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, chunk.Data) {
			t.Error("data not equal")
		}
		if chunk.Length != len(data) {
			t.Errorf("invalid length %d", chunk.Length)
		}

		_, err = chunker.Next()
		if err != io.EOF {
			t.Error("expected io.EOF error")
		}
	})

	t.Run("all zeros", func(t *testing.T) {
		data := make([]byte, 10240)

		chunker, err := NewChunker(bytes.NewReader(data), 256,
			WithMinSize(64),
			WithMaxSize(1024),
			WithNormalization(1),
		)
		if err != nil {
			t.Fatal(err)
		}

		var totalLength int
		for {
			chunk, err := chunker.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if chunk.Length != 1024 {
				t.Errorf("expected chunk length 1024 for all zeros, got %d", chunk.Length)
			}
			totalLength += chunk.Length
		}

		if totalLength != 10240 {
			t.Errorf("expected total length 10240, got %d", totalLength)
		}
	})
}

func BenchmarkChunker(b *testing.B) {
	sizes := []struct {
		size int
		name string
	}{
		{1 << 10, "1k"},
		{4 << 10, "4k"},
		{16 << 10, "16k"},
		{32 << 10, "32k"},
		{64 << 10, "64k"},
		{128 << 10, "128k"},
		{256 << 10, "256k"},
		{512 << 10, "512k"},
		{1 << 20, "1M"},
		{4 << 20, "4M"},
		{16 << 20, "16M"},
		{32 << 20, "32M"},
		{64 << 20, "64M"},
		{128 << 20, "128M"},
		{512 << 20, "512M"},
		{1 << 30, "1G"},
	}

	for _, s := range sizes {
		b.Run(s.name, func(b *testing.B) {
			benchmarkChunker(b, s.size)
		})
	}
}

func benchmarkChunker(b *testing.B, size int) {
	rng := rand.New(rand.NewSource(1))
	data := make([]byte, size)
	rng.Read(data)

	r := bytes.NewReader(data)
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	chunker, err := NewChunker(r, 1<<20)
	if err != nil {
		b.Fatal(err)
	}

	var nchunks int64

	for i := 0; i < b.N; i++ {
		r.Reset(data)
		chunker.Reset(r)

		for {
			_, err := chunker.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				b.Fatal(err)
			}
			nchunks++
		}
	}
	b.ReportMetric(float64(nchunks)/float64(b.N), "chunks")
}

func randBytes(n int, seed int64) []byte {
	b := make([]byte, n)
	rnd := rand.New(rand.NewSource(seed))
	rnd.Read(b)
	return b
}
