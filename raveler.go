package main

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"

	"image"
	"image/color"
	_ "image/png"

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

// Returns the first Z of the block in which this z is located.
func zhead(z int) int {
	nz := z / *blocksize
	return *blocksize * nz
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

func processRavelerExport(sp_to_seg, seg_to_body, sp_dir, out_dir string) error {
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
	return transformImages(sp2body, sp_dir, out_dir)
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

func transformImages(sp2body map[Superpixel]uint64, sp_dir, out_dir string) error {
	// Make sure output directory exists
	if fileinfo, err := os.Stat(out_dir); os.IsNotExist(err) {
		fmt.Printf("Creating output directory: %s\n", out_dir)
		err := os.MkdirAll(out_dir, 0744)
		if err != nil {
			return fmt.Errorf("Can't make output directory: %s\n", err.Error())
		}
	} else if !fileinfo.IsDir() {
		return fmt.Errorf("Supplied output path (%s) is not a directory.", out_dir)
	}

	fileregex, err := regexp.Compile(`[[:digit:]]+\.png$`)
	if err != nil {
		return err
	}

	// Read all image files, transform them, and write to output directory.
	var (
		outbuf  []uint64
		bytebuf []byte
		nx, ny  int // # of voxels in X and Y direction
		zoffset int // the starting z of current output buffer
		zInBuf  int // # of Z slices stored in output buffer
	)
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
		if outbuf == nil {
			nx = b.Dx()
			ny = b.Dy()
			nxy := nx * ny
			outbuf = make([]uint64, *blocksize*nxy, *blocksize*nxy)
			bytebuf = make([]byte, *blocksize*nxy*8, *blocksize*nxy*8)
		} else if nx != b.Dx() || ny != b.Dy() {
			return fmt.Errorf("superpixel image changes sizes: expected %d x %d and got %d x %d: %s",
				nx, ny, b.Dx(), b.Dy(), fullpath)
		}

		// Write past buffer if we are no longer in it
		if zInBuf != 0 && zhead(z) != zoffset {
			if err := writeBuffer(out_dir, nx, ny, zoffset, outbuf, bytebuf); err != nil {
				return err
			}
			for i := range outbuf {
				outbuf[i] = 0
			}
			zoffset = zhead(z)
			zInBuf = 0
		}

		// Iterate through the image and store body into our output buffer.
		zInBuf++
		zbuf := z % *blocksize // z offset into the buffer

		var label uint32
		var body uint64
		var found bool
		sp := Superpixel{Slice: uint32(z)}
		i := 0
		for y := b.Min.Y; y < b.Max.Y; y++ {
			for x := b.Min.X; x < b.Max.X; x++ {
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
				outbuf[zbuf*nx*ny+i] = body
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
		if err := writeBuffer(out_dir, nx, ny, zoffset, outbuf, bytebuf); err != nil {
			return err
		}
	}
	return nil
}

func writeBuffer(out_dir string, nx, ny, zoffset int, outbuf []uint64, bytebuf []byte) error {
	tlog := NewTimeLog()

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
	base := fmt.Sprintf("bodies-z%06d-%dx%dx%d.%s", zoffset, nx, ny, *blocksize, ext)
	filename := filepath.Join(out_dir, base)

	// Store the outbut buffer to preallocated byte slice.
	for i, label := range outbuf {
		binary.LittleEndian.PutUint64(bytebuf[i*8:i*8+8], label)
	}

	// Setup file for write
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	// Write the data
	switch *compression {
	case "none":
		_, err = f.Write(bytebuf)
		if err != nil {
			return err
		}
	case "lz4":
		compressed := make([]byte, lz4.CompressBound(bytebuf))
		var n, outSize int
		if outSize, err = lz4.Compress(bytebuf, compressed); err != nil {
			return err
		}
		compressed = compressed[:outSize]
		if n, err = f.Write(compressed); err != nil {
			return err
		}
		if n != outSize {
			return fmt.Errorf("Only able to write %d of %d lz4 compressed bytes\n", n, outSize)
		}
	case "gzip":
		gw := gzip.NewWriter(f)
		if _, err = gw.Write(bytebuf); err != nil {
			return err
		}
		if err = gw.Close(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown compression type %q", *compression)
	}
	tlog.Printf("Wrote %s", filename)
	return nil
}
