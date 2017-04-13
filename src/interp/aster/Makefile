CC=g++

all: testASTERLatLon

ASTERLatLon.o: ASTERLatLon.c
	$(CC) -o $@ -c $<
testASTERLatLon.o: testASTERLatLon.c
	$(CC) -o $@ -c $<
testASTERLatLon: ASTERLatLon.o testASTERLatLon.o
	$(CC) -o ./$@ $+

clean:
	rm *.o testASTERLatLon
