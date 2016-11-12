#all variables suffixed with 1 denote HDF4, 2 HDF5
CC2=h5cc
CC1=h4cc
CFLAGS=-c -g -O0 -Wall -Werror
LINKFLAGS= -g
INCLUDE2=/usr/local/hdf5/include/
LIB1=/usr/local/hdf-4.2.12/hdf4/lib
LIB2=/usr/local/
DEPS=main.o libTERRA.o MOPITTmain.o CERESmain.o MODISmain.o
TARGET=TERRA_repackage

all: $(TARGET)

$(TARGET): $(DEPS)
	$(CC2) $(LINKFLAGS) $(DEPS) -L$(LIB1) -lmfhdf -ldf -lz -lhdfeos -ljpeg -o $(TARGET)
	
main.o: main.c
	$(CC2) $(CFLAGS) main.c
	
libTERRA.o: libTERRA.c
	$(CC1) $(CFLAGS) -I$(INCLUDE2) libTERRA.c
	
MOPITTmain.o: MOPITTmain.c
	$(CC2) $(CFLAGS) MOPITTmain.c
	
CERESmain.o: CERESmain.c
	$(CC1) $(CFLAGS) -I$(INCLUDE2) CERESmain.c
	
MODISmain.o: MODISmain.c
	$(CC1) $(CFLAGS) -I$(INCLUDE2) MODISmain.c

clean:
	rm $(TARGET) *.o
	
run:
	./$(TARGET) "./InputFiles/MOP01-20160217-L1V3.50.0.he5" "./InputFiles/CER_BDS_Terra-FM1_Edition3_300307.20160531" "./InputFiles/MOD021KM.A2016275.0000.006.2016275133438.hdf" out2.h5 0 0
