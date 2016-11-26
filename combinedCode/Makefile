#all variables suffixed with 1 denote HDF4, 2 HDF5
CC2=h5cc
CC1=h4cc
CFLAGS=-c -g -O0 -Wall
LINKFLAGS= -g
INCLUDE2=/usr/local/
LIB1=/usr/local/
LIB2=/usr/local/
DEPS=main.o libTERRA.o MOPITTmain.o CERESmain.o MODISmain.o ASTERmain.o #MISRmain.o
TARGET=TERRA_repackage

all: $(TARGET)

$(TARGET): $(DEPS)
	$(CC2) $(LINKFLAGS) $(DEPS) -L$(LIB1) -lmfhdf -ldf -lz -ljpeg -o $(TARGET)
	
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
	
ASTERmain.o: ASTERmain.c
	$(CC1) $(CFLAGS) -I$(INCLUDE2) ASTERmain.c
	
MISRmain.o: MISRmain.c
	$(CC1) $(CFLAGS) -I$(INCLUDE2) MISRmain.c

clean:
	rm $(TARGET) *.o
	
run:
	./$(TARGET) out.h5
