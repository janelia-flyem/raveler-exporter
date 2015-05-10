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
	"strings"
	"time"
)

var (
	// Display usage if true.
	dryrun   = flag.Bool("dryrun", false, "")
	showHelp = flag.Bool("help", false, "")

	outdir = flag.String("outdir", "", "")
	url    = flag.String("url", "", "")

	slabX = flag.Int("slabX", 512, "")
	slabY = flag.Int("slabY", 512, "")
	slabZ = flag.Int("slabZ", 32, "")

	roiBlocksize = flag.Int("roiblocksize", 32, "")

	bodyoffset = flag.Int("bodyoffset", 0, "")

	minz = flag.Int("minz", 0, "")
	maxz = flag.Int("maxz", math.MaxInt32, "")

	// How the output should be compressed
	compression = flag.String("compress", "none", "")

	roiFile = flag.String("roi", "", "")

	// output file for cluster script
	script      = flag.String("script", "", "")
	binpath     = flag.String("binpath", "/groups/flyem/proj/builds/cluster2015/bin", "")
	filesPerJob = flag.Int("filesperjob", *slabZ*5, "")
)

const helpMessage = `
raveler-exporter converts Raveler superpixel-based images + maps to a series of compressed label slabs.

Usage: raveler-exporter [options] <superpixel-to-segment-map> <segment-to-body-map> <superpixels directory> 

		-outdir         =string   Output directory for file output
		-url            =string   POST URL for DVID, e.g., "http://dvidserver.com/api/653/dataname"

	    -compress       =string   Compression for output files.  default "none" but allows "gzip" and "lz4".

	    -script         =string   Generate batch script for running on SGE cluster (requires -directory)
	    -filesperjob    =number   Number of Z slices that should be assigned to one cluster job if using -script.
	    -binpath        =string   Absolute path to this executable for script creation.

	    -roi            =string   Absolute path to a ROI JSON containing sorted (in ascending order) block index spans
	    -roiblocksize   =number   Size of each ROI block in pixels diameter (default 32)

	    -bodyoffset     =number   Offset to apply to body labels, e.g., if 1000 all body labels are incremented by 1000.

	    -slabX          =number   Size along X of label slab (default 512)
	    -slabY          =number   Size along Y of label slab (default 512)
	    -slabZ          =number   Size along Z of label slab (default 32)

	    -minz           =number   Starting Z slice to process.
	    -maxz           =number   Ending Z slice to process.

	    -dryrun         (flag)    Don't write files or send POST requests to DVID
	-h, -help           (flag)    Show help message

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

	if *showHelp || flag.NArg() != 3 {
		flag.Usage()
		os.Exit(0)
	}

	if *slabZ < 1 {
		fmt.Printf("Thickness must be >= 1 Z slice\n")
		os.Exit(1)
	}

	if *url == "" && *outdir == "" {
		fmt.Printf("Must either use -url and/or -outdir for output!\n")
		os.Exit(1)
	}

	args := flag.Args()
	if *script != "" {
		if *outdir == "" {
			fmt.Printf("Script output requires -outdir as well\n")
			os.Exit(1)
		}
		if err := generateScript(args[0], args[1], args[2], *outdir); err != nil {
			fmt.Printf("Error generating script: %s\n", err.Error())
			os.Exit(1)
		}
		os.Exit(0)
	}

	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)

	if err := processRavelerExport(args[0], args[1], args[2]); err != nil {
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

	var options []string
	if *slabX != 512 {
		options = append(options, fmt.Sprintf("-slabX=%d", *slabX))
	}
	if *slabY != 512 {
		options = append(options, fmt.Sprintf("-slabY=%d", *slabY))
	}
	if *slabZ != 32 {
		options = append(options, fmt.Sprintf("-slabZ=%d", *slabZ))
	}

	if *roiFile != "" {
		options = append(options, fmt.Sprintf("-roi=%s", *roiFile))
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
			zlast := zoffset + *slabZ - 1

			if curFiles >= *filesPerJob {
				cmd := fmt.Sprintf(`%s/raveler-exporter %s -minz=%d -maxz=%d %s %s %s %s`, *binpath,
					strings.Join(options, " "), zstart, zlast, sp_to_seg, seg_to_body, sp_dir, out_dir)

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
		zlast := zoffset + *slabZ - 1

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
