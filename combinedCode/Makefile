# All variables suffixed with 1 denote HDF4, 2 HDF5
# This makefile is currently set up to be run on the 
# Blue Waters computer.
CC=/sw/hdf5-1.8.16/bin/h5cc
#CC1=/sw/hdf-4.2.12/bin/h4cc
#CC1=/projects/sciteam/jq0/TerraFusion/basicFusion/libraries/bin/h4cc
#CFLAGS=-c -g -O0 -Wall -std=c11
CFLAGS=-c -g -O0 -Wall -std=c99
LINKFLAGS= -g -std=c99
#LINKFLAGS= -g -std=c11
INCLUDE1=/sw/hdf-4.2.12/include
INCLUDE2=
LIB1=/sw/hdf-4.2.12/lib
LIB2=
DEPS=main.o libTERRA.o MOPITTmain.o CERESmain.o MODISmain.o ASTERmain.o MISRmain.o
TARGET=TERRArepackage

all: $(TARGET)

$(TARGET): $(DEPS)
	$(CC) $(LINKFLAGS) $(DEPS) -L$(LIB1) -I$(INCLUDE1) -lmfhdf -ldf -lz -ljpeg -o $(TARGET)
	
main.o: main.c
	$(CC) $(CFLAGS) -L$(LIB1) -I$(INCLUDE1)  main.c
	
libTERRA.o: libTERRA.c
	$(CC) $(CFLAGS) -I$(INCLUDE1) libTERRA.c
	
MOPITTmain.o: MOPITTmain.c
	$(CC) $(CFLAGS) -I$(INCLUDE1) MOPITTmain.c
	
CERESmain.o: CERESmain.c
	$(CC) $(CFLAGS) -I$(INCLUDE1) CERESmain.c
	
MODISmain.o: MODISmain.c
	$(CC) $(CFLAGS) -I$(INCLUDE1) MODISmain.c
	
ASTERmain.o: ASTERmain.c
	$(CC) $(CFLAGS) -I$(INCLUDE1) ASTERmain.c
	
MISRmain.o: MISRmain.c
	$(CC) $(CFLAGS) -I$(INCLUDE1) MISRmain.c

clean:
	rm $(TARGET) *.o
	
run:
	./$(TARGET) out.h5
