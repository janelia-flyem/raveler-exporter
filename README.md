# raveler-exporter

Exports Raveler superpixel-based images + maps to a series of optionally compressed label slabs.

Usage: raveler-exporter [options] *superpixel-to-segment-map* *segment-to-body-map* *superpixels-directory* *output-directory*

	    -compression =string   Compression for output files.  default "gzip" but allows "lz4" and "uncompressed".
	    -thickness   =number   Number of Z slices should be combined to form each label slab.
	-h, -help        (flag)    Show help message

We assume there is enough RAM to hold the both mapping files.
