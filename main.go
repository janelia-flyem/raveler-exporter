package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"time"
)

var (
	// Display usage if true.
	showHelp = flag.Bool("help", false, "")

	// Number of Z-slices that should be combined into one slab.
	blocksize = flag.Int("blocksize", 32, "")

	minz = flag.Int("minz", 0, "")
	maxz = flag.Int("maxz", math.MaxInt32, "")

	// How the output should be compressed
	compression = flag.String("compress", "gzip", "")

	// output file for cluster script
	script      = flag.String("script", "", "")
	binpath     = flag.String("binpath", "/groups/flyem/proj/builds/cluster2015/bin", "")
	filesPerJob = flag.Int("filesperjob", *blocksize*5, "")
)

const helpMessage = `
raveler-exporter converts Raveler superpixel-based images + maps to a series of compressed label slabs.

Usage: raveler-exporter [options] <superpixel-to-segment-map> <segment-to-body-map> <superpixels directory> <output directory>

	    -compression =string   Compression for output files.  default "gzip" but allows "lz4" and "none".

	    -script      =string   Generate batch script for running on SGE cluster
	    -filesperjob =number   Number of Z slices that should be assigned to one cluster job if using -script.
	    -binpath     =string   Absolute path to this executable for script creation.

	    -blocksize   =number   Number of Z slices should be combined to form each label slab.
	    -minz        =number   Starting Z slice to process.
	    -maxz        =number   Ending Z slice to process.
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

	args := flag.Args()
	if *script != "" {
		if err := generateScript(args[0], args[1], args[2], args[3]); err != nil {
			fmt.Printf("Error generating script: %s\n", err.Error())
			os.Exit(1)
		}
		os.Exit(0)
	}

	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)

	if err := processRavelerExport(args[0], args[1], args[2], args[3]); err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	}
}

func generateScript(sp_to_seg, seg_to_body, sp_dir, out_dir string) error {
	fmt.Printf("Generating batcn script: %s\n", *script)

	file, err := os.Create(*script)
	if err != nil {
		return fmt.Errorf("Could not open %q to write it: %s", *script, err.Error())
	}
	defer file.Close()

	fileregex, err := regexp.Compile(`[[:digit:]]+\.png$`)
	if err != nil {
		return err
	}

	var (
		jobnum           int
		zstart, curFiles int
		zoffset          int // the starting z of current output buffer
		first            bool
	)
	first = true
	err = filepath.Walk(sp_dir, func(fullpath string, f os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("Error traversing the superpixel image directory @ %s: %s\n", fullpath, err.Error())
			os.Exit(1)
		}
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

		if first {
			zstart = z
			first = false
		}

		// Good stopping place given block sizes?
		if zhead(z) != zoffset {
			zlast := zoffset + *blocksize - 1

			if curFiles >= *filesPerJob {
				cmd := fmt.Sprintf(`%s/raveler-exporter -minz=%d -maxz=%d %s %s %s %s`, *binpath, zstart, zlast,
					sp_to_seg, seg_to_body, sp_dir, out_dir)

				jobname := fmt.Sprintf("ravelerexport-%d", jobnum)
				job := fmt.Sprintf(`qsub -pe batch 16 -N %s -j y -o %s.log -b y -cwd -V '%s > %s.out'`, jobname, jobname, cmd, jobname)
				job += "\n"

				if _, err := file.WriteString(job); err != nil {
					return err
				}
				zstart = z
				curFiles = 0
				jobnum++
			}
		}

		// Count this file and see if we have enough to print a job in the script
		zoffset = zhead(z)
		curFiles++
		return nil
	})
	if err != nil {
		fmt.Printf("Error traversing superpixel directory: %s\n", err.Error())
		os.Exit(1)
	}

	if curFiles > 0 {
		zlast := zoffset + *blocksize - 1

		cmd := fmt.Sprintf(`%s/raveler-exporter -minz=%d -maxz=%d %s %s %s %s`, *binpath, zstart, zlast,
			sp_to_seg, seg_to_body, sp_dir, out_dir)

		jobname := fmt.Sprintf("ravelerexport-%d", jobnum)
		job := fmt.Sprintf(`qsub -pe batch 16 -N %s -j y -o /dev/null -b y -cwd -V '%s'`, jobname, cmd)

		if _, err := file.WriteString(job); err != nil {
			return err
		}
	}
	return nil
}
