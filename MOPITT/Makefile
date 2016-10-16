CC=h5cc
CFLAGS=-c -Wall
LINKFLAGS=
INCLUDE=/usr/local/hdf5/include/
DEPS=main.o libTERRA.o
TARGET=TERRA_repackage

all: $(TARGET)

$(TARGET): $(DEPS)
	$(CC) $(LINKFLAGS) $(DEPS) -o $(TARGET)
	
main.o: main.c
	$(CC) $(CFLAGS) -I$(INCLUDE) main.c
	
libTERRA.o: libTERRA.c
	$(CC) $(CFLAGS) -I$(INCLUDE) libTERRA.c
	
clean:
	rm $(TARGET) *.o
