package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"time"

	"image"
	"image/color"
	_ "image/png"
	"io/ioutil"

	lz4 "github.com/janelia-flyem/go/golz4"
)

// SuperpixelFormat notes whether superpixel ids, if present,
// are in 16-bit or 24-bit values.
type SuperpixelFormat uint8

// Enumerate the types of superpixel id formats
const (
	SuperpixelNone SuperpixelFormat = iota
	Superpixel16Bits
	Superpixel24Bits
)

// Superpixel is a Raveler-oriented description of a superpixel that
// breaks a unique superpixel id into two components: a slice and a
// unique label within that slice.
type Superpixel struct {
	Slice uint32
	Label uint32
}

// Span is (Z, Y, X0, X1).
// TODO -- Consolidate with dvid.RLE since both handle run-length encodings in X, although
// dvid.RLE handles voxel coordinates not block (chunk) coordinates.
type Span [4]int

func (s Span) String() string {
	return fmt.Sprintf("[%d, %d, %d, %d]", s[0], s[1], s[2], s[3])
}

// Extends returns true and modifies the span if the given point
// is one more in x direction than this span.  Else it returns false.
func (s *Span) Extends(x, y, z int) bool {
	if s == nil || (*s)[0] != z || (*s)[1] != y || (*s)[3] != x-1 {
		return false
	}
	(*s)[3] = x
	return true
}

func (s Span) Unpack() (z, y, x0, x1 int) {
	return s[0], s[1], s[2], s[3]
}

func (s Span) Less(block [3]int) bool {
	if s[0] < block[2] {
		return true
	}
	if s[0] > block[2] {
		return false
	}
	if s[1] < block[1] {
		return true
	}
	if s[1] > block[1] {
		return false
	}
	if s[3] < block[0] {
		return true
	}
	return false
}

func (s Span) Includes(block [3]int) bool {
	if s[0] != block[2] {
		return false
	}
	if s[1] != block[1] {
		return false
	}
	if s[2] > block[0] || s[3] < block[0] {
		return false
	}
	return true
}

// Returns the current span index and whether given block is included in span.
func seekSpan(block [3]int, roi []Span, curSpanI int) (int, bool) {
	numSpans := len(roi)
	if curSpanI >= numSpans {
		return curSpanI, false
	}

	// Keep going through spans until we are equal to or past the chunk point.
	for {
		curSpan := roi[curSpanI]
		if curSpan.Less(block) {
			curSpanI++
		} else {
			if curSpan.Includes(block) {
				return curSpanI, true
			} else {
				return curSpanI, false
			}
		}
		if curSpanI >= numSpans {
			return curSpanI, false
		}
	}
}

// Returns the first Z of the block in which this z is located.
func zhead(z int) int {
	nz := z / *slabZ
	return *slabZ * nz
}

// getSuperpixelId returns the superpixel id given a color.  This routine handles 32-bit
// and 16-bit superpixel images.  From the Raveler documentation:
//    16-bit: pixel intensity is superpixel id
//    32-bit: superpixel id = R + (256 * G) + (65536 * B)
func getSuperpixelId(c color.Color, format SuperpixelFormat) (id uint32, err error) {
	switch format {
	case Superpixel24Bits:
		switch c.(type) {
		case color.NRGBA:
			v := c.(color.NRGBA)
			id = uint32(v.B)
			id <<= 8
			id |= uint32(v.G)
			id <<= 8
			id |= uint32(v.R)
		case color.RGBA:
			v := c.(color.RGBA)
			id = uint32(v.B)
			id <<= 8
			id |= uint32(v.G)
			id <<= 8
			id |= uint32(v.R)
		default:
			err = fmt.Errorf("expected 32-bit RGBA superpixels, got", reflect.TypeOf(c))
		}
	case Superpixel16Bits:
		id = uint32(c.(color.Gray16).Y)
	default:
		err = fmt.Errorf("unknown superpixel format %v", format)
	}
	return
}

func processRavelerExport(sp_to_seg, seg_to_body, sp_dir string) error {
	// If we have roi, load it.
	var roi []Span

	if *roiFile != "" {
		f, err := os.Open(*roiFile)
		if err != nil {
			return err
		}
		defer f.Close()
		jsonBytes, err := ioutil.ReadAll(f)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(jsonBytes, &roi); err != nil {
			return fmt.Errorf("Error trying to parse JSON ROI: %s", err.Error())
		}
	}

	// Get the seg->body map
	seg2body, err := loadSegBodyMap(seg_to_body)
	if err != nil {
		return err
	}

	tlog := NewTimeLog()

	sp2body := make(map[Superpixel]uint64, len(seg2body))

	var slice, superpixel32 uint32
	var segment, body uint64

	// Get the sp->seg map and compute the sp->body mapping.
	file, err := os.Open(sp_to_seg)
	if err != nil {
		return fmt.Errorf("Could not open superpixel->segment map: %s", sp_to_seg)
	}
	defer file.Close()
	lineReader := bufio.NewReader(file)
	linenum := 0

	fmt.Printf("Processing superpixel->segment map: %s\n", sp_to_seg)
	for {
		line, err := lineReader.ReadString('\n')
		if err != nil {
			break
		}
		if line[0] == ' ' || line[0] == '#' {
			continue
		}
		if _, err := fmt.Sscanf(line, "%d %d %d", &slice, &superpixel32, &segment); err != nil {
			return fmt.Errorf("Error loading superpixel->segment map, line %d in %s", linenum, sp_to_seg)
		}
		if superpixel32 == 0 {
			continue
		}
		if superpixel32 > 0x0000000000FFFFFF {
			return fmt.Errorf("Error in line %d: superpixel id exceeds 24-bit value!", linenum)
		}
		var found bool
		body, found = seg2body[segment]
		if !found {
			return fmt.Errorf("Segment (%d) in %s not found in %s", segment, sp_to_seg, seg_to_body)
		}

		// Store this mapping.
		sp2body[Superpixel{slice, superpixel32}] = body

		linenum++
		if linenum%1000000 == 0 {
			fmt.Printf("Loaded %d superpixel->body mappings\n", linenum)
		}
	}
	tlog.Printf("Completed loading superpixel to body mappings")

	// Delete the seg->body map.
	seg2body = nil

	// Read in an transform each superpixel image file.
	return transformImages(sp2body, roi, sp_dir)
}

func loadSegBodyMap(filename string) (map[uint64]uint64, error) {
	tlog := NewTimeLog()

	segmentToBodyMap := make(map[uint64]uint64, 100000)
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("Could not open segment->body map: %s", filename)
	}
	defer file.Close()
	linenum := 0
	lineReader := bufio.NewReader(file)
	for {
		line, err := lineReader.ReadString('\n')
		if err != nil {
			break
		}
		if line[0] == ' ' || line[0] == '#' {
			continue
		}
		var segment, body uint64
		if _, err := fmt.Sscanf(line, "%d %d", &segment, &body); err != nil {
			return nil, fmt.Errorf("Error loading segment->body map, line %d in %s", linenum, filename)
		}
		segmentToBodyMap[segment] = body
		linenum++

		if linenum%100000 == 0 {
			fmt.Printf("Loaded %d lines of segment->body map\n", linenum)
		}
	}
	tlog.Printf("Loaded segment->body map, %s", filename)
	return segmentToBodyMap, nil
}

type layerT struct {
	buf  []uint64
	nx   int
	ny   int
	nz   int
	nxy  int
	nxyz int
}

func transformImages(sp2body map[Superpixel]uint64, roi []Span, sp_dir string) error {
	// Make sure output directory exists if it's specified.
	if *outdir != "" {
		if fileinfo, err := os.Stat(*outdir); os.IsNotExist(err) {
			fmt.Printf("Creating output directory: %s\n", *outdir)
			err := os.MkdirAll(*outdir, 0744)
			if err != nil {
				return fmt.Errorf("Can't make output directory: %s\n", err.Error())
			}
		} else if !fileinfo.IsDir() {
			return fmt.Errorf("Supplied output path (%s) is not a directory.", *outdir)
		}
	}

	fileregex, err := regexp.Compile(`[[:digit:]]+\.png$`)
	if err != nil {
		return err
	}

	// Read all image files, transform them, and write to output directory.
	var (
		layer   layerT
		zoffset int // the starting z of current output buffer
		zInBuf  int // # of Z slices stored in output buffer
		first   bool
	)
	first = true
	err = filepath.Walk(sp_dir, func(fullpath string, f os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("Error traversing the superpixel image directory @ %s: %s\n", fullpath, err.Error())
			os.Exit(1)
		}
		tlog := NewTimeLog()

		ext := filepath.Ext(fullpath)
		if ext != ".png" {
			fmt.Printf("Skipping transformation of non-PNG file: %s\n", fullpath)
			return nil
		}

		// Parse the filename to get the Z slice.
		rfrag := fileregex.FindString(fullpath) // gets everything from number through end of extension.
		if len(rfrag) < 5 {
			return fmt.Errorf("error parsing Z slice in filename %q", fullpath)
		}
		rfrag = rfrag[:len(rfrag)-4]
		z, err := strconv.Atoi(rfrag)
		if err != nil {
			return fmt.Errorf("error parsing Z in filename %q: %s\n", fullpath, err.Error())
		}

		// Skip files that aren't within our processing range.
		if z < *minz || z > *maxz {
			return nil
		}

		// Load the superpixel PNG image
		file, err := os.Open(fullpath)
		defer file.Close()
		if err != nil {
			return fmt.Errorf("Unable to open superpixel image %q", fullpath)
		}
		img, iformat, err := image.Decode(file)
		if iformat != "png" {
			return fmt.Errorf("superpixel image was not PNG formatted")
		}

		// Image type determines the type of superpixel we will decode.
		var format SuperpixelFormat
		switch typedImg := img.(type) {
		case *image.Gray16:
			format = Superpixel16Bits
		case *image.RGBA, *image.NRGBA:
			format = Superpixel24Bits
		default:
			return fmt.Errorf("Unable to decode superpixel image of type %T", typedImg)
		}

		// Allocate buffer if not already allocated.
		b := img.Bounds()
		if layer.buf == nil {
			layer.nx, layer.ny = b.Dx(), b.Dy()
			layer.nz = *slabZ
			layer.nxy = layer.nx * layer.ny
			layer.nxyz = layer.nxy * layer.nz
			layer.buf = make([]uint64, layer.nxyz, layer.nxyz)
		} else if layer.nx != b.Dx() || layer.ny != b.Dy() {
			return fmt.Errorf("superpixel image changes sizes: expected %d x %d and got %d x %d: %s",
				layer.nx, layer.ny, b.Dx(), b.Dy(), fullpath)
		}

		if first {
			zoffset = zhead(z)
			first = false
		}

		// Write past buffer if we are no longer in it
		if zInBuf != 0 && zhead(z) != zoffset {
			if err := writeLayer(layer, zoffset); err != nil {
				return err
			}
			for i := range layer.buf {
				layer.buf[i] = 0
			}
			zoffset = zhead(z)
			zInBuf = 0
		}

		// Iterate through the image and store body into our output buffer.
		zInBuf++
		zbuf := z % layer.nz // z offset into the buffer

		var label uint32
		var body uint64
		var found bool
		var block [3]int

		block[0] = b.Min.X / *roiBlocksize
		block[1] = b.Min.Y / *roiBlocksize
		block[2] = z / *roiBlocksize
		initSpan, _ := seekSpan(block, roi, 0)

		sp := Superpixel{Slice: uint32(z)}
		i := 0
		for y := b.Min.Y; y < b.Max.Y; y++ {
			curSpan := initSpan
			for x := b.Min.X; x < b.Max.X; x++ {
				if roi != nil {
					block[0] = x / *roiBlocksize
					block[1] = y / *roiBlocksize
					var inROI bool
					curSpan, inROI = seekSpan(block, roi, curSpan)
					if !inROI {
						i++
						continue
					}
				}
				if label, err = getSuperpixelId(img.At(x, y), format); err != nil {
					return err
				}
				if label == 0 {
					body = 0
				} else {
					sp.Label = label
					body, found = sp2body[sp]
					if !found {
						fmt.Printf("Could not find superpixel (%d, %d) in mapping files.  Setting to body 0.\n", sp.Slice, sp.Label)
						body = 0
					}
				}
				if *bodyoffset != 0 {
					body += uint64(*bodyoffset)
				}
				layer.buf[zbuf*layer.nxy+i] = body
				i++
			}
		}
		tlog.Printf("Processed superpixel image, %s", filepath.Base(fullpath))
		return nil
	})
	if err != nil {
		return err
	}

	// Make sure we write any unsaved data in output buffer
	if zInBuf != 0 {
		if err := writeLayer(layer, zoffset); err != nil {
			return err
		}
	}
	return nil
}

func writeLayer(layer layerT, zoffset int) error {
	tlog := NewTimeLog()

	// Compute some slab indexing
	sxBytes := *slabX * 8
	sxyBytes := *slabY * sxBytes
	sxyzBytes := *slabZ * sxyBytes

	// Iterate through all slabs in this layer, writing each one either to file or DVID via http POST
	for oy := 0; oy < layer.ny; oy += *slabY {
		endY := oy + *slabY
		if endY > layer.ny {
			endY = layer.ny
		}
		for ox := 0; ox < layer.nx; ox += *slabX {
			endX := ox + *slabX
			if endX > layer.nx {
				endX = layer.nx
			}

			// Store data from slab into the POST buffer
			slabBuf := make([]byte, sxyzBytes, sxyzBytes)
			for z := 0; z < *slabZ; z++ {
				sy := 0
				for y := oy; y < endY; y++ {
					sx := 0
					for x := ox; x < endX; x++ {
						layerI := z*layer.nxy + y*layer.nx + x
						si := z*sxyBytes + sy*sxBytes + sx*8
						binary.LittleEndian.PutUint64(slabBuf[si:si+8], layer.buf[layerI])
						sx++
					}
					sy++
				}
			}

			// Send the data
			if *url != "" {
				if err := writeDVID(slabBuf, ox, oy, zoffset); err != nil {
					return err
				}
			}
			if *outdir != "" {
				if err := writeFile(slabBuf, ox, oy, zoffset); err != nil {
					return err
				}
			}
		}
	}

	tlog.Printf("Wrote layer starting at Z %d", zoffset)
	return nil
}

func writeDVID(slabBuf []byte, ox, oy, oz int) error {
	url := fmt.Sprintf("%s/raw/0_1_2/%d_%d_%d/%d_%d_%d?throttle=on", *url, *slabX, *slabY, *slabZ, ox, oy, oz)
	switch *compression {
	case "gzip", "lz4":
		url += "&compression=" + *compression
	}

	out, err := compress(slabBuf)
	if err != nil {
		return err
	}

	fmt.Printf("POSTing %d bytes to %s\n", len(out), url)
	if *dryrun {
		return nil
	}

	for {
		r, err := http.Post(url, "application/octet-stream", bytes.NewBuffer(out))
		if err != nil {
			return err
		}
		switch r.StatusCode {
		case http.StatusOK:
			fmt.Printf("POSTed successfully %d bytes to %s\n", len(out), url)
			return nil
		case http.StatusServiceUnavailable:
			// Retry after variable delay
			timeout := time.Duration(30 + rand.Intn(30))
			time.Sleep(timeout * time.Second)
			fmt.Printf("Unsuccessful POST of slab @ (%d,%d,%d) %d bytes.  Retrying in %d seconds\n",
				ox, oy, oz, len(out), timeout)
		default:
			// We have a problem
			return fmt.Errorf("Received bad status from POST on %q: %d\n", url, r.StatusCode)
		}
	}
}

func writeFile(slabBuf []byte, ox, oy, oz int) error {
	// Compute the output file name
	var ext string
	switch *compression {
	case "none":
		ext = "dat"
	case "lz4":
		ext = "lz4"
	case "gzip":
		ext = "gz"
	default:
		return fmt.Errorf("unknown compression type %q", *compression)
	}
	base := fmt.Sprintf("bodies-%6dx%6dx%6d+%6d+%6d+%6d.%s", *slabX, *slabY, *slabZ, ox, oy, oz, ext)
	filename := filepath.Join(*outdir, base)

	fmt.Printf("Writing data to %s\n", filename)
	if *dryrun {
		return nil
	}

	// Setup file for write
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	// Compress and write
	out, err := compress(slabBuf)
	if err != nil {
		return err
	}

	_, err = f.Write(out)
	if err != nil {
		return err
	}
	return nil
}

func compress(slabBuf []byte) ([]byte, error) {
	switch *compression {

	case "none":
		return slabBuf, nil

	case "lz4":
		compressed := make([]byte, lz4.CompressBound(slabBuf))
		outsize, err := lz4.Compress(slabBuf, compressed)
		if err != nil {
			return nil, err
		}
		return compressed[:outsize], nil

	case "gzip":
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		if _, err := gw.Write(slabBuf); err != nil {
			return nil, err
		}
		if err := gw.Close(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil

	default:
		return nil, fmt.Errorf("unknown compression type %q", *compression)
	}
}
