# DON'T DELETE THIS FILE. 
# This is a template for roger.
CC=/sw/hdf5-1.8.16/bin/h5cc
CFLAGS=-c -g -O0 -Wall -std=c99
LINKFLAGS= -g -std=c99 
INCLUDE1=/sw/hdf-4.2.12/include
INCLUDE2=
LIB1=/sw/hdf-4.2.12/lib
LIB2=
TARGET=./bin/basicFusion
SRCDIR=./src
OBJDIR=./obj
MODISINTERP_DIR=./src/interp/modis
ASTERINTERP_DIR=./src/interp/aster
DEPS=$(OBJDIR)/main.o $(OBJDIR)/libTERRA.o $(OBJDIR)/MOPITT.o $(OBJDIR)/CERES.o $(OBJDIR)/MODIS.o $(OBJDIR)/ASTER.o $(OBJDIR)/MISR.o $(MODISINTERP_DIR)/MODISLatLon.o $(ASTERINTERP_DIR)/ASTERLatLon.o

all: $(TARGET)

$(TARGET): $(DEPS)
	$(CC) $(LINKFLAGS) $(DEPS) -L$(LIB1) -I$(INCLUDE1) -lhdf5_hl -lhdf5 -lmfhdf -ldf -lz -ljpeg -o $(TARGET)
	
$(OBJDIR)/main.o: $(SRCDIR)/main.c
	$(CC) $(CFLAGS) -L$(LIB1) -I$(INCLUDE1) $(SRCDIR)/main.c -o $(OBJDIR)/main.o
	
$(OBJDIR)/libTERRA.o: $(SRCDIR)/libTERRA.c
	$(CC) $(CFLAGS) -I$(INCLUDE1) $(SRCDIR)/libTERRA.c -o $(OBJDIR)/libTERRA.o
	
$(OBJDIR)/MOPITT.o: $(SRCDIR)/MOPITT.c
	$(CC) $(CFLAGS) -I$(INCLUDE1) $(SRCDIR)/MOPITT.c -o $(OBJDIR)/MOPITT.o
	
$(OBJDIR)/CERES.o: $(SRCDIR)/CERES.c
	$(CC) $(CFLAGS) -I$(INCLUDE1) $(SRCDIR)/CERES.c -o $(OBJDIR)/CERES.o
	
$(OBJDIR)/MODIS.o: $(SRCDIR)/MODIS.c
	$(CC) $(CFLAGS) -I$(INCLUDE1) $(SRCDIR)/MODIS.c -o $(OBJDIR)/MODIS.o
	
$(OBJDIR)/ASTER.o: $(SRCDIR)/ASTER.c
	$(CC) $(CFLAGS) -I$(INCLUDE1) $(SRCDIR)/ASTER.c -o $(OBJDIR)/ASTER.o
	
$(OBJDIR)/MISR.o: $(SRCDIR)/MISR.c
	$(CC) $(CFLAGS) -I$(INCLUDE1) $(SRCDIR)/MISR.c -o $(OBJDIR)/MISR.o

$(OBJDIR)/MODISLatLon.o: $(MODISINTERP_DIR)/MODISLatLon.c
	$(CC) $(CFLAGS) -I$(INCLUDE1) $(MODISINTERP_DIR)/MODISLatLon.c -o $(OBJDIR)/MODISLatLon.o

$(OBJDIR)/ASTERLatLon.o: $(ASTERINTERP_DIR)/ASTERLatLon.c
	$(CC) $(CFLAGS) -I$(INCLUDE1) $(ASTERINTERP_DIR)/ASTERLatLon.c -o $(OBJDIR)/ASTERLatLon.o


clean:
	rm -f $(TARGET) $(OBJDIR)/*.o
	
run:
	$(TARGET) out.h5
