package main

import (
	"bufio"
	"fmt"
	"image"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
)

// Superpixel is a Raveler-oriented description of a superpixel that
// breaks a unique superpixel id into two components: a slice and a
// unique label within that slice.
type Superpixel struct {
	Slice uint32
	Label uint32
}

func loadSegBodyMap(filename string) (map[uint64]uint64, error) {
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
	fmt.Printf("Loaded segment->body map: %s\n", filename)
	return segmentToBodyMap, nil
}

// Returns the first Z of the block in which this z is located.
func zhead(z int) int {
	nz := z / *blocksize
	return *blocksize * nz
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

	fileregex, err := regexp.Compile(`[[:digit:]]\.png$`)
	if err != nil {
		return err
	}

	// Read all image files, transform them, and write to output directory.
	var (
		outbuf  []uint64
		nx, ny  int // # of voxels in X and Y direction
		zoffset int // the starting z of current output buffer
		zInBuf  int // # of Z slices stored in output buffer
	)
	err = filepath.Walk(sp_dir, func(fullpath string, f os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("Error traversing the superpixel image directory @ %s: %s\n", fullpath, err.Error())
			os.Exit(1)
		}
		fmt.Printf("Processing %s...\n", fullpath)
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
			bytebuf = make([]byte, *blocksize*nxy*4, *blocksize*nxy*4)
		} else if nx != b.Dx() || ny != b.Dy() {
			return fmt.Errorf("superpixel image changes sizes: expected %d x %d and got %d x %d: %s",
				nx, ny, b.Dx(), b.Dy(), fullpath)
		}

		// Write past buffer if we are no longer in it
		if zInBuf != 0 && zhead(z) != zoffset {
			if err := writeBuffer(out_dir, nx, ny, zoffset, outbuf); err != nil {
				return err
			}
			for i := range outbuf {
				outbuf[i] = 0
			}
			zInBuf = 0
		}

		// Iterate through the image and store body into our output buffer.
		zInBuf++
		zoffset = zhead(z)
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

		return nil
	})
	if err != nil {
		return err
	}

	// Make sure we write any unsaved data in output buffer
	if zInBuf != 0 {
		if err := writeBuffer(out_dir, nx, ny, zoffset, outbuf); err != nil {
			return err
		}
	}
	return nil
}

func processRavelerExport(sp_to_seg, seg_to_body, sp_dir, out_dir string) error {
	// Get the seg->body map
	seg2body, err := loadSegBodyMap(seg_to_body)
	if err != nil {
		return err
	}

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

	// Delete the seg->body map.
	seg2body = nil

	// Read in an transform each superpixel image file.
	return transformImages(sp2body, sp_dir, out_dir)
}
