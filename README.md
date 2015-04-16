# raveler-exporter

Exports Raveler superpixel-based images + maps to a series of optionally compressed label slabs.

Usage: raveler-exporter [options] *superpixel-to-segment-map* *segment-to-body-map* *superpixels-directory* *output-directory*

	    -compression =string   Compression for output files.  default "gzip" but allows "lz4" and "none".

	    -script      =string   Generate batch script for running on SGE cluster
	    -filesperjob =number   Number of Z slices that should be assigned to one cluster job if using -script.
	    -binpath     =string   Absolute path to this executable for script creation.

	    -blocksize   =number   Number of Z slices should be combined to form each label slab.
	    -minz        =number   Starting Z slice to process.
	    -maxz        =number   Ending Z slice to process.
	-h, -help        (flag)    Show help message

We assume there is enough RAM to hold the both mapping files.
