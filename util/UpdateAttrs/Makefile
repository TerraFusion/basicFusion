# This makefile is currently set up to be run on the 
# Blue Waters computer.


# MODIFY THIS VARIABLE
#----------------------------

#BFDIR should be an absolute path to your basicFusion directory
BFDIR=/u/sciteam/ymuqun/scratch/bf-test-all/basicFusion  
#----------------------------

CC=gcc
CFLAGS=-c -Wall -std=c99
LINKFLAGS= -g -std=c99 -static
#INCLUDE1=$(BFDIR)/externLib/hdf/include/
INCLUDE1=/u/sciteam/clipp/basicFusion/externLib/hdf/include
INCLUDE2=.
LIB1=/u/sciteam/clipp/basicFusion/externLib/hdf/lib
LIB2=.
TARGET=./BFUpdateAttrs
SRCDIR=.
OBJDIR=.

DEPS=$(OBJDIR)/h5_cha_attr.o

all: $(TARGET)

$(TARGET): $(DEPS)
	$(CC) $(LINKFLAGS) $(DEPS) -L$(LIB1) -lhdf5_hl -lhdf5 -lz -lm -ldl -lrt -o $(TARGET)
	
$(OBJDIR)/h5_cha_attr.o: $(SRCDIR)/h5_cha_attr.c
	$(CC) $(CFLAGS) -I$(INCLUDE1) $(SRCDIR)/h5_cha_attr.c -o $(OBJDIR)/h5_cha_attr.o
	
clean:
	rm -f $(TARGET) $(OBJDIR)/*.o

