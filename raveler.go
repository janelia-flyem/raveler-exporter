/*
	This file supports reading and writing Raveler superpixel images.
*/

package main

import (
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

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

var bytebuf []byte

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

func writeBuffer(out_dir string, nx, ny, zoffset int, outbuf []uint64) error {
	// Compute the output file name
	base := fmt.Sprintf("bodies-z%06d-%dx%dx%d.dat", zoffset, nx, ny, *blocksize)
	filename := filepath.Join(out_dir, base)

	// Store the outbut buffer to preallocated byte slice.
	for i, label := range outbuf {
		binary.LittleEndian.PutUint64(bytebuf[i*4:i*4+4], label)
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
	return nil
}
