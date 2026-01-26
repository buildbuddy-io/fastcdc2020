// Package fastcdc implements the FastCDC 2020 content-defined chunking algorithm.
// See https://ieeexplore.ieee.org/document/9055082 by Wen Xia, et al.
//
// This implementation uses the 2-byte rolling optimization described in section 3.7
// of the paper for improved performance.
package fastcdc

import (
	"errors"
	"io"
	"math/bits"
)

const (
	// absoluteMinSize and absoluteMaxSize are just sanity bounds for chunk sizes
	// to give a helpful error message. The actual limits will be determined by
	// the AverageSize/Normalization combination.
	absoluteMinSize = 64
	absoluteMaxSize = 1 << 30

	// Normalization defaults to 2 from testing using Bazel build
	// artifacts, since it provided the best balance of deduplication
	// and chunk size consistency.
	//
	// Stats:
	// Algorithm         │ Dedup%   │ Saved        │ Chunks/File avg
	// ─────────────────────────────────────────────────────────────
	// normalization-0  │   30.37% │     93.87 GB │     33.4 │
	// normalization-1  │   31.30% │     96.73 GB │     34.4 │
	// normalization-2  │   32.09% │     99.19 GB │     38.3 │
	// normalization-3  │   32.07% │     99.10 GB │     41.4 │
	//
	// Chunk Size Distribution:
	// Algorithm         │ Avg size   │ Stdev      |
	// ─────────────────────────────────────────────
	// normalization-0  │  705.72 KB │  526.70 KB │
	// normalization-1  │  684.91 KB │  376.66 KB │
	// normalization-2  │  615.49 KB │  242.25 KB │
	// normalization-3  │  570.18 KB │  176.29 KB │

	defaultNormalization = 2
)

type Option func(*options)

type options struct {
	averageSize          int
	minSize              int
	maxSize              int
	normalization        int
	disableNormalization bool
	seed                 uint64
	bufSize              int
}

// WithMinSize overrides the minimum chunk size (defaults to averageSize / 4).
func WithMinSize(size int) Option {
	return func(o *options) {
		o.minSize = size
	}
}

// WithMaxSize overrides the maximum chunk size (defaults to averageSize * 4).
func WithMaxSize(size int) Option {
	return func(o *options) {
		o.maxSize = size
	}
}

// WithNormalization sets the normalization level from 0-3 (defaults to 2).
//
// Higher normalization levels produce chunks closer to the average size by
// making it harder to chunk at small sizes and harder to chunk at large sizes.
// This could reduce de-duplication, but can make chunk sizes more predictable.
//
//	0: Normalization disabled
//	1: Fewer chunks outside desired range
//	2: Most chunks match desired size (recommended)
//	3: Nearly all chunks are the desired size
func WithNormalization(level int) Option {
	return func(o *options) {
		o.normalization = level
		o.disableNormalization = level == 0
	}
}

// WithSeed applies an XOR mask to the global gear tables to prevent fingerprinting
// attacks that infer content from chunk sizes.
func WithSeed(seed uint64) Option {
	return func(o *options) {
		o.seed = seed
	}
}

// WithBufferSize sets the read buffer size (defaults to maxSize * 2).
// Larger buffers reduce read syscalls. Must exceed maxSize.
func WithBufferSize(size int) Option {
	return func(o *options) {
		o.bufSize = size
	}
}

func (o *options) setDefaults() {
	if o.minSize == 0 {
		o.minSize = o.averageSize / 4
	}
	if o.maxSize == 0 {
		o.maxSize = o.averageSize * 4
	}
	if o.bufSize == 0 {
		o.bufSize = o.maxSize * 2
	}
	if !o.disableNormalization && o.normalization == 0 {
		o.normalization = defaultNormalization
	}
}

func (o *options) validate() error {
	if o.averageSize < absoluteMinSize || o.averageSize > absoluteMaxSize {
		return errors.New("AverageSize must be in range 64B to 1GiB")
	}
	if o.averageSize <= 0 || (o.averageSize&(o.averageSize-1)) != 0 {
		return errors.New("AverageSize must be a power of 2")
	}
	if o.minSize < absoluteMinSize || o.minSize > absoluteMaxSize {
		return errors.New("MinSize must be in range 64B to 1GiB")
	}
	if o.maxSize < absoluteMinSize || o.maxSize > absoluteMaxSize {
		return errors.New("MaxSize must be in range 64B to 1GiB")
	}
	if o.maxSize <= o.minSize {
		return errors.New("MinSize must be less than MaxSize")
	}
	if o.averageSize > o.maxSize || o.averageSize < o.minSize {
		return errors.New("AverageSize must be between MinSize and MaxSize")
	}
	if !o.disableNormalization && (o.normalization < 0 || o.normalization > 3) {
		return errors.New("Normalization must be 0, 1, 2, or 3")
	}
	if o.bufSize <= o.maxSize {
		return errors.New("BufferSize must be greater than MaxSize")
	}
	return nil
}

// Chunk holds the result of a single content-defined chunk.
type Chunk struct {
	Offset      int    // Byte position in the stream where this chunk starts.
	Length      int    // Size of the chunk in bytes.
	Data        []byte // Raw chunk bytes. Only valid until the next call to Next.
	Fingerprint uint64 // Final gear hash value at the chunk boundary.
}

// Chunker splits a byte stream into variable-sized chunks using FastCDC 2020.
type Chunker struct {
	minSize       int
	maxSize       int
	normalizeSize int

	maskSmall        uint64
	maskLarge        uint64
	maskSmallShifted uint64
	maskLargeShifted uint64

	gear        [256]uint64
	gearShifted [256]uint64

	reader io.Reader

	buf       []byte
	bufCursor int
	bufEnd    int
	streamPos int
	readerEOF bool
}

// NewChunker creates a new Chunker with the given average chunk size.
// The averageSize must be a power of 2 and must be in the range 64B to 1GiB.
// High normalization reduces the range of allowed values for average size.
// Other options have sensible defaults.
func NewChunker(rd io.Reader, averageSize int, opts ...Option) (*Chunker, error) {
	o := &options{averageSize: averageSize}
	for _, opt := range opts {
		opt(o)
	}

	o.setDefaults()
	if err := o.validate(); err != nil {
		return nil, err
	}

	seedGear := [256]uint64{}
	seedGearShifted := [256]uint64{}
	if o.seed != 0 {
		shiftedSeed := o.seed << 1
		for i := range gear {
			seedGear[i] = gear[i] ^ o.seed
			seedGearShifted[i] = gearShifted[i] ^ shiftedSeed
		}
	} else {
		seedGear = gear
		seedGearShifted = gearShifted
	}

	normalization := o.normalization
	if o.disableNormalization {
		normalization = 0
	}
	log2Avg := bits.TrailingZeros(uint(o.averageSize))
	smallBits := log2Avg + normalization
	largeBits := log2Avg - normalization
	if smallBits > 25 || largeBits < 5 {
		return nil, errors.New("AverageSize/Normalization combination exceeds mask table bounds")
	}

	maskS := masks[smallBits]
	maskL := masks[largeBits]

	chunker := &Chunker{
		minSize:          o.minSize,
		maxSize:          o.maxSize,
		normalizeSize:    o.averageSize,
		maskSmall:        maskS,
		maskLarge:        maskL,
		maskSmallShifted: maskS << 1,
		maskLargeShifted: maskL << 1,
		reader:           rd,
		buf:              make([]byte, o.bufSize),
		bufCursor:        o.bufSize,
		bufEnd:           o.bufSize,
		gear:             seedGear,
		gearShifted:      seedGearShifted,
	}

	return chunker, nil
}

// Reset reinitializes the chunker with a new reader.
func (c *Chunker) Reset(rd io.Reader) {
	c.reader = rd
	c.streamPos = 0
	c.readerEOF = false

	// bufCursor indicates the position to read from.
	// placing it at the end means the buffer is empty
	// and needs to be filled.
	c.bufCursor = len(c.buf)
	c.bufEnd = len(c.buf)
}

func (c *Chunker) fillBuffer() error {
	availableToRead := c.bufEnd - c.bufCursor

	// We know that the maximum chunk we can produce
	// is c.maxSize, so if we have at least that much
	// data available, we don't need to read more.
	if availableToRead >= c.maxSize {
		return nil
	}

	_ = copy(c.buf[:availableToRead], c.buf[c.bufCursor:])
	c.bufCursor = 0

	if c.readerEOF {
		c.bufEnd = availableToRead
		return nil
	}

	bytesRead, err := io.ReadFull(c.reader, c.buf[availableToRead:])
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		c.bufEnd = availableToRead + bytesRead
		c.readerEOF = true
		return nil
	}
	return err
}

// Next returns the next chunk, or io.EOF when the stream is exhausted.
// The chunk's Data slice is only valid until the next call to Next.
func (c *Chunker) Next() (Chunk, error) {
	if err := c.fillBuffer(); err != nil {
		return Chunk{}, err
	}
	if c.bufEnd == 0 {
		return Chunk{}, io.EOF
	}

	length, fp := c.cut(c.buf[c.bufCursor:c.bufEnd])

	chunk := Chunk{
		Offset:      c.streamPos,
		Length:      length,
		Data:        c.buf[c.bufCursor : c.bufCursor+length],
		Fingerprint: fp,
	}

	c.bufCursor += length
	c.streamPos += length

	return chunk, nil
}

func (c *Chunker) cut(data []byte) (int, uint64) {
	localGear := c.gear
	localGearShifted := c.gearShifted

	dataLen := len(data)
	if dataLen <= c.minSize {
		return dataLen, 0
	}

	maxBoundary := dataLen
	if maxBoundary > c.maxSize {
		maxBoundary = c.maxSize
	}
	normalizeBoundary := c.normalizeSize
	if maxBoundary < normalizeBoundary {
		normalizeBoundary = maxBoundary
	}

	_ = data[maxBoundary-1] // https://go101.org/optimizations/5-bce.html

	// Round down to even for 2-byte-at-a-time processing
	scanStart := c.minSize &^ 1
	normalizeAt := normalizeBoundary &^ 1
	scanEnd := maxBoundary &^ 1

	var fingerprint uint64

	// Use smaller mask (harder to match) until normalize point
	for i := scanStart; i < normalizeAt; i += 2 {
		fingerprint = (fingerprint << 2) + localGearShifted[data[i]]
		if (fingerprint & c.maskSmallShifted) == 0 {
			return i, fingerprint
		}
		fingerprint = fingerprint + localGear[data[i+1]]
		if (fingerprint & c.maskSmall) == 0 {
			return i + 1, fingerprint
		}
	}

	// Use larger mask (easier to match) after normalize point
	for i := normalizeAt; i < scanEnd; i += 2 {
		fingerprint = (fingerprint << 2) + localGearShifted[data[i]]
		if (fingerprint & c.maskLargeShifted) == 0 {
			return i, fingerprint
		}
		fingerprint = fingerprint + localGear[data[i+1]]
		if (fingerprint & c.maskLarge) == 0 {
			return i + 1, fingerprint
		}
	}

	return maxBoundary, fingerprint
}

// masks holds the normalized chunking masks from the FastCDC 2020 paper (Table II).
// Index corresponds to log2(chunk_size), e.g., masks[13] is for 8KB chunks.
var masks = [26]uint64{
	0,                  // 0: padding
	0,                  // 1: padding
	0,                  // 2: padding
	0,                  // 3: padding
	0,                  // 4: padding
	0x0000000001804110, // 5: used for NC 3
	0x0000000001803110, // 6: 64B
	0x0000000018035100, // 7: 128B
	0x0000001800035300, // 8: 256B
	0x0000019000353000, // 9: 512B
	0x0000590003530000, // 10: 1KB
	0x0000d90003530000, // 11: 2KB
	0x0000d90103530000, // 12: 4KB
	0x0000d90303530000, // 13: 8KB
	0x0000d90313530000, // 14: 16KB
	0x0000d90f03530000, // 15: 32KB
	0x0000d90303537000, // 16: 64KB
	0x0000d90703537000, // 17: 128KB
	0x0000d90707537000, // 18: 256KB
	0x0000d91707537000, // 19: 512KB
	0x0000d91747537000, // 20: 1MB
	0x0000d91767537000, // 21: 2MB
	0x0000d93767537000, // 22: 4MB
	0x0000d93777537000, // 23: 8MB
	0x0000d93777577000, // 24: 16MB
	0x0000db3777577000, // 25: used for NC 3
}

// gear is the lookup table for the rolling hash, derived from the FastCDC 2020 paper.
var gear = [256]uint64{
	0x3b5d3c7d207e37dc, 0x784d68ba91123086, 0xcd52880f882e7298, 0xeacf8e4e19fdcca7,
	0xc31f385dfbd1632b, 0x1d5f27001e25abe6, 0x83130bde3c9ad991, 0xc4b225676e9b7649,
	0xaa329b29e08eb499, 0xb67fcbd21e577d58, 0x0027baaada2acf6b, 0xe3ef2d5ac73c2226,
	0x0890f24d6ed312b7, 0xa809e036851d7c7e, 0xf0a6fe5e0013d81b, 0x1d026304452cec14,
	0x03864632648e248f, 0xcdaacf3dcd92b9b4, 0xf5e012e63c187856, 0x8862f9d3821c00b6,
	0xa82f7338750f6f8a, 0x1e583dc6c1cb0b6f, 0x7a3145b69743a7f1, 0xabb20fee404807eb,
	0xb14b3cfe07b83a5d, 0xb9dc27898adb9a0f, 0x3703f5e91baa62be, 0xcf0bb866815f7d98,
	0x3d9867c41ea9dcd3, 0x1be1fa65442bf22c, 0x14300da4c55631d9, 0xe698e9cbc6545c99,
	0x4763107ec64e92a5, 0xc65821fc65696a24, 0x76196c064822f0b7, 0x485be841f3525e01,
	0xf652bc9c85974ff5, 0xcad8352face9e3e9, 0x2a6ed1dceb35e98e, 0xc6f483badc11680f,
	0x3cfd8c17e9cf12f1, 0x89b83c5e2ea56471, 0xae665cfd24e392a9, 0xec33c4e504cb8915,
	0x3fb9b15fc9fe7451, 0xd7fd1fd1945f2195, 0x31ade0853443efd8, 0x255efc9863e1e2d2,
	0x10eab6008d5642cf, 0x46f04863257ac804, 0xa52dc42a789a27d3, 0xdaaadf9ce77af565,
	0x6b479cd53d87febb, 0x6309e2d3f93db72f, 0xc5738ffbaa1ff9d6, 0x6bd57f3f25af7968,
	0x67605486d90d0a4a, 0xe14d0b9663bfbdae, 0xb7bbd8d816eb0414, 0xdef8a4f16b35a116,
	0xe7932d85aaaffed6, 0x08161cbae90cfd48, 0x855507beb294f08b, 0x91234ea6ffd399b2,
	0xad70cf4b2435f302, 0xd289a97565bc2d27, 0x8e558437ffca99de, 0x96d2704b7115c040,
	0x0889bbcdfc660e41, 0x5e0d4e67dc92128d, 0x72a9f8917063ed97, 0x438b69d409e016e3,
	0xdf4fed8a5d8a4397, 0x00f41dcf41d403f7, 0x4814eb038e52603f, 0x9dafbacc58e2d651,
	0xfe2f458e4be170af, 0x4457ec414df6a940, 0x06e62f1451123314, 0xbd1014d173ba92cc,
	0xdef318e25ed57760, 0x9fea0de9dfca8525, 0x459de1e76c20624b, 0xaeec189617e2d666,
	0x126a2c06ab5a83cb, 0xb1321532360f6132, 0x65421503dbb40123, 0x2d67c287ea089ab3,
	0x6c93bff5a56bd6b6, 0x4ffb2036cab6d98d, 0xce7b785b1be7ad4f, 0xedb42ef6189fd163,
	0xdc905288703988f6, 0x365f9c1d2c691884, 0xc640583680d99bfe, 0x3cd4624c07593ec6,
	0x7f1ea8d85d7c5805, 0x014842d480b57149, 0x0b649bcb5a828688, 0xbcd5708ed79b18f0,
	0xe987c862fbd2f2f0, 0x982731671f0cd82c, 0xbaf13e8b16d8c063, 0x8ea3109cbd951bba,
	0xd141045bfb385cad, 0x2acbc1a0af1f7d30, 0xe6444d89df03bfdf, 0xa18cc771b8188ff9,
	0x9834429db01c39bb, 0x214add07fe086a1f, 0x8f07c19b1f6b3ff9, 0x56a297b1bf4ffe55,
	0x94d558e493c54fc7, 0x40bfc24c764552cb, 0x931a706f8a8520cb, 0x32229d322935bd52,
	0x2560d0f5dc4fefaf, 0x9dbcc48355969bb6, 0x0fd81c3985c0b56a, 0xe03817e1560f2bda,
	0xc1bb4f81d892b2d5, 0xb0c4864f4e28d2d7, 0x3ecc49f9d9d6c263, 0x51307e99b52ba65e,
	0x8af2b688da84a752, 0xf5d72523b91b20b6, 0x6d95ff1ff4634806, 0x562f21555458339a,
	0xc0ce47f889336346, 0x487823e5089b40d8, 0xe4727c7ebc6d9592, 0x5a8f7277e94970ba,
	0xfca2f406b1c8bb50, 0x5b1f8a95f1791070, 0xd304af9fc9028605, 0x5440ab7fc930e748,
	0x312d25fbca2ab5a1, 0x10f4a4b234a4d575, 0x90301d55047e7473, 0x3b6372886c61591e,
	0x293402b77c444e06, 0x451f34a4d3e97dd7, 0x3158d814d81bc57b, 0x034942425b9bda69,
	0xe2032ff9e532d9bb, 0x62ae066b8b2179e5, 0x9545e10c2f8d71d8, 0x7ff7483eb2d23fc0,
	0x00945fcebdc98d86, 0x8764bbbe99b26ca2, 0x1b1ec62284c0bfc3, 0x58e0fcc4f0aa362b,
	0x5f4abefa878d458d, 0xfd74ac2f9607c519, 0xa4e3fb37df8cbfa9, 0xbf697e43cac574e5,
	0x86f14a3f68f4cd53, 0x24a23d076f1ce522, 0xe725cd8048868cc8, 0xbf3c729eb2464362,
	0xd8f6cd57b3cc1ed8, 0x6329e52425541577, 0x62aa688ad5ae1ac0, 0x0a242566269bf845,
	0x168b1a4753aca74b, 0xf789afefff2e7e3c, 0x6c3362093b6fccdb, 0x4ce8f50bd28c09b2,
	0x006a2db95ae8aa93, 0x975b0d623c3d1a8c, 0x18605d3935338c5b, 0x5bb6f6136cad3c71,
	0x0f53a20701f8d8a6, 0xab8c5ad2e7e93c67, 0x40b5ac5127acaa29, 0x8c7bf63c2075895f,
	0x78bd9f7e014a805c, 0xb2c9e9f4f9c8c032, 0xefd6049827eb91f3, 0x2be459f482c16fbd,
	0xd92ce0c5745aaa8c, 0x0aaa8fb298d965b9, 0x2b37f92c6c803b15, 0x8c54a5e94e0f0e78,
	0x95f9b6e90c0a3032, 0xe7939faa436c7874, 0xd16bfe8f6a8a40c9, 0x44982b86263fd2fa,
	0xe285fb39f984e583, 0x779a8df72d7619d3, 0xf2d79a8de8d5dd1e, 0xd1037354d66684e2,
	0x004c82a4e668a8e5, 0x31d40a7668b044e6, 0xd70578538bd02c11, 0xdb45431078c5f482,
	0x977121bb7f6a51ad, 0x73d5ccbd34eff8dd, 0xe437a07d356e17cd, 0x47b2782043c95627,
	0x9fb251413e41d49a, 0xccd70b60652513d3, 0x1c95b31e8a1b49b2, 0xcae73dfd1bcb4c1b,
	0x34d98331b1f5b70f, 0x784e39f22338d92f, 0x18613d4a064df420, 0xf1d8dae25f0bcebe,
	0x33f77c15ae855efc, 0x3c88b3b912eb109c, 0x956a2ec96bafeea5, 0x1aa005b5e0ad0e87,
	0x5500d70527c4bb8e, 0xe36c57196421cc44, 0x13c4d286cc36ee39, 0x5654a23d818b2a81,
	0x77b1dc13d161abdc, 0x734f44de5f8d5eb5, 0x60717e174a6c89a2, 0xd47d9649266a211e,
	0x5b13a4322bb69e90, 0xf7669609f8b5fc3c, 0x21e6ac55bedcdac9, 0x9b56b62b61166dea,
	0xf48f66b939797e9c, 0x35f332f9c0e6ae9a, 0xcc733f6a9a878db0, 0x3da161e41cc108c2,
	0xb7d74ae535914d51, 0x4d493b0b11d36469, 0xce264d1dfba9741a, 0xa9d1f2dc7436dc06,
	0x70738016604c2a27, 0x231d36e96e93f3d5, 0x7666881197838d19, 0x4a2a83090aaad40c,
	0xf1e761591668b35d, 0x7363236497f730a7, 0x301080e37379dd4d, 0x502dea2971827042,
	0xc2c5eb858f32625f, 0x786afb9edfafbdff, 0xdaee0d868490b2a4, 0x617366b3268609f6,
	0xae0e35a0fe46173e, 0xd1a07de93e824f11, 0x079b8b115ea4cca8, 0x93a99274558faebb,
	0xfb1e6e22e08a03b3, 0xea635fdba3698dd0, 0xcf53659328503a5c, 0xcde3b31e6fd5d780,
	0x8e3e4221d3614413, 0xef14d0d86bf1a22c, 0xe1d830d3f16c5ddb, 0xaabd2b2a451504e1,
}

// gearShifted is gear with each value left-shifted by 1 for the 2-byte optimization.
var gearShifted [256]uint64

func init() {
	for i := range 256 {
		gearShifted[i] = gear[i] << 1
	}
}
