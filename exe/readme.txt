Directory to contain all of the executables
1. To test at roger(cluster at NCSA cyber group),  one needs to copy roger_large_input.txt and roger_small_input.txt under ../inputFileDebug to this directory.
2. Run with the full orbit data, use roger_large_input.txt. Run with the subset orbit data for debugging, use roger_small_input.txt.
./TERRArepackage test.h5 roger_large_input.txt
./TERRArepackage test.h5 roger_small_input.txt

