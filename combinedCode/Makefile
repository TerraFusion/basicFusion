# All variables suffixed with 1 denote HDF4, 2 HDF5
# This makefile is currently set up to be run on the 
# Blue Waters computer.
CC=cc
CC1=/projects/sciteam/jq0/TerraFusion/basicFusion/libraries/bin/h4cc
CFLAGS=-c -g -O0 -Wall -std=c11
LINKFLAGS= -g -std=c11
INCLUDE1=/projects/sciteam/jq0/TerraFusion/basicFusion/libraries/include
INCLUDE2=
LIB1=/projects/sciteam/jq0/TerraFusion/basicFusion/libraries/lib
LIB2=
DEPS=main.o libTERRA.o MOPITTmain.o CERESmain.o MODISmain.o ASTERmain.o
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
