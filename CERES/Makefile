CC=h4cc
INCLUDE1=/usr/local/HDF-EOS/hdfeos/include
INCLUDE2=/usr/local/hdf-4.2.12/hdf4/include
LIB_EOS=/usr/local/HDF-EOS/hdfeos/hdfeos2/lib
TARGET=CERESout
DEPS=testCERES.o readHDFfuncs.o

all: $(TARGET)

$(TARGET): $(DEPS)
	$(CC) $(DEPS) -L$(LIB_EOS) -o $(TARGET) -lmfhdf -ldf -lz -lhdfeos -lm

testCERES.o: testCERES.c
	$(CC) -c -g -I$(INCLUDE1) -I$(INCLUDE2) testCERES.c -g
	
readHDFfuncs.o: readHDFfuncs.c
	$(CC) -w -c -g -I$(INCLUDE2) readHDFfuncs.c -g



clean:
	rm *.o $(TARGET)
