CC = h5cc
CFLAGS = -g -Wall
INCLUDE = /usr/local/hdf5/include/

TARGET = MOPITT_HDF

all: $(TARGET)

$(TARGET): $(TARGET).c
	$(CC) $(CFLAGS) -I $(INCLUDE) -o $(TARGET) $(TARGET).c
	
clean:
	rm $(TARGET) *.o
