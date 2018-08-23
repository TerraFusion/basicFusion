import argparse
import subprocess as sp
import os
import bfutils

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("orbit", type=int)

    args = parser.parse_args()
    batch_array_key = 'AWS_BATCH_JOB_ARRAY_INDEX'
    if batch_array_key in os.environ:
        arr_idx = int(os.environ[batch_array_key])
        args.orbit = args.orbit + arr_idx
        print("Detected as an array job.\nIdx: {}\nInferred orbit: {}".format(arr_idx, args.orbit))

    # Open input file
    infilelistdir = str( (args.orbit / 1000) * 1000 )
    infile="/bf/infilelist/{}/input{}.txt".format(infilelistdir, args.orbit)
    with open(  infile, "r" ) as f:
        for line in f:
            if line[0] == "/" or line[1] == "/":
                # Grab file from S3 into local storage. For now we simply use
                # the s3 path as both the input s3 path and local POSIX
                # destination path.

                line = line.strip()
                aws_call = ['aws', 's3', 'cp', "--quiet", "s3:/{}".format(line), line ]
                print( ' '.join(aws_call))
                sp.check_call( aws_call )


    # Print all downloaded files
    for dirname, dirnames, filenames in os.walk( '/terra-l1b' ):
        for filename in filenames:
            print( os.path.join( dirname, filename) + '\n')

    outfilename = bfutils.file.get_bf_filename( args.orbit )
    starttime = bfutils.file.orbit_start(args.orbit)
    outfiledir = "{}.{}".format( starttime[0:4], starttime[4:6] )
    bfoutpath = os.path.join( outfiledir, outfilename )

    local_outfile="/bf/{}".format(bfoutpath)

    # Call BF
    bf_call = ["basicfusion", local_outfile, infile, "/bf/orbit_info.bin"]
    print( ' '.join(bf_call))
    sp.check_call(bf_call)
    print( sp.check_output( ['stat', outfile] ) )

    # TODO
    # Create separate stdout/err log files and send the logs to S3 along with BF files

    # Output
    aws_call = ['aws', 's3', 'cp', "--quiet", local_outfile, 's3://basicfusion/{}'.format( bfoutpath ) ]
    print( ' '.join(aws_call) )
    sp.check_call( aws_call )

if __name__ == "__main__":
    main()
