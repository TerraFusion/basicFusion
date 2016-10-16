CC=h4cc
INCLUDE=/usr/local/hdf-4.2.12/hdf4/include
LIB=/usr/local/hdf4.2.12/hdf4/lib
TARGET=rd
DEPS=readHDFfuncs.o testCERES.o

all: $(TARGET)

$(TARGET): $(DEPS)
	$(CC) -echo -s $(DEPS) -o $(TARGET) -lmfhdf -ldf -lz

readHDFfuncs.o: readHDFfuncs.c
	$(CC) -echo -c readHDFfuncs.c

testCERES.o: testCERES.c
	$(CC) -echo -c testCERES.c

clean:
	rm *.o $(TARGET)
