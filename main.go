package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"
)

var (
	// Display usage if true.
	showHelp = flag.Bool("help", false, "")

	// Number of Z-slices that should be combined into one slab.
	blocksize = flag.Int("blocksize", 32, "")

	// How the output should be compressed
	compression = flag.String("compress", "gzip", "")
)

const helpMessage = `
raveler-exporter converts Raveler superpixel-based images + maps to a series of compressed label slabs.

Usage: raveler-exporter [options] <superpixel-to-segment-map> <segment-to-body-map> <superpixels directory> <output directory>

	    -compression =string   Compression for output files.  default "gzip" but allows "lz4" and "uncompressed".
	    -thickness   =number   Number of Z slices should be combined to form each label slab.
	-h, -help        (flag)    Show help message

We assume there is enough RAM to hold the both mapping files.
`

// TimeLog adds elapsed time to logging.
// Example:
//     mylog := NewTimeLog()
//     ...
//     mylog.Printf("stuff happened")  // Appends elapsed time from NewTimeLog() to message.
type TimeLog struct {
	start time.Time
}

func NewTimeLog() TimeLog {
	return TimeLog{time.Now()}
}

func (t TimeLog) Printf(format string, args ...interface{}) {
	log.Printf(format+": %s\n", append(args, time.Since(t.start))...)
}

var usage = func() {
	fmt.Printf(helpMessage)
}

func currentDir() string {
	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatalln("Could not get current directory:", err)
	}
	return currentDir
}

func main() {
	flag.BoolVar(showHelp, "h", false, "Show help message")
	flag.Usage = usage
	flag.Parse()

	if *showHelp || flag.NArg() != 4 {
		flag.Usage()
		os.Exit(0)
	}

	if *blocksize < 1 {
		fmt.Printf("Thickness must be >= 1 Z slice\n")
		os.Exit(1)
	}

	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)

	args := flag.Args()
	if err := processRavelerExport(args[0], args[1], args[2], args[3]); err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	}
}
