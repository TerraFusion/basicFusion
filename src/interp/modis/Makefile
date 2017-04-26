CC=gcc

all: testMODISLatLon

MODISLatLon.o: MODISLatLon.c
	$(CC) -o $@ -c $<
testMODISLatLon.o: testMODISLatLon.c
	$(CC) -o $@ -c $<
testMODISLatLon: MODISLatLon.o testMODISLatLon.o
	$(CC) -o ./$@ $+ -lm

clean:
	rm *.o testMODISLatLon
