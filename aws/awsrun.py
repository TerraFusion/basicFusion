import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("orbit", type=int)

    args = parser.parse_args()

    # Open input file
    infilelistdir = str( (args.orbit / 1000) * 1000 )
    infile="./{}_s3/input{}.txt".format(infilelistdir, args.orbit)
    with open(  infile, "r" ) as f:
        for line in f:
            if line[0] == "/" or line[1] == "/":
                # Grab file from S3
                print("grabbing {}".format(line))


    # Call BF
    print("basicfusion /bf/basicfusion/orbit_info.bin {} /path/to/out.h5".format(infile))

if __name__ == "__main__":
    main()
