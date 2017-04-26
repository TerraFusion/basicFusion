#include <stdio.h>
#include <stdlib.h>
#include "MODISLatLon.h"

int main(int argc, char ** argv) {

	int nRow = 2030;
	int nCol = 1354;
	int scanSize = 10;

	double * oriLat;
	double * oriLon;

	double * newLat;
	double * newLon;

	if(NULL == (oriLat = (double *)malloc(sizeof(double) * nRow * nCol))) {
		printf("Out of memeory for oriLat\n");
		exit(1);
	}

	if(NULL == (oriLon = (double *)malloc(sizeof(double) * nRow * nCol))) {
		printf("Out of memeory for oriLon\n");
		exit(1);
	}

	if(NULL == (newLat = (double *)malloc(sizeof(double) * 4 * nRow * nCol))) {
		printf("Out of memeory for newLat\n");
		exit(1);
	}

	if(NULL == (newLon = (double *)malloc(sizeof(double) * 4 * nRow * nCol))) {
		printf("Out of memeory for newLon\n");
		exit(1);
	}

	char * fileName = "MODIS1630.csv";
	FILE * input;

	if(NULL == (input = fopen(fileName, "r"))) {
		printf("Cannot read input file\n");
		exit(1);
	}

	for(int i = 0; i < nRow; i++) {
		for(int j = 0; j < nCol; j++) {
			fscanf(input, "%lf,%lf\n", oriLat + i * nCol + j, oriLon + i * nCol + j);
		}
	}

//	upscaleLatLonPlanar(oriLat, oriLon, nRow, nCol, scanSize, newLat, newLon);
	upscaleLatLonSpherical(oriLat, oriLon, nRow, nCol, scanSize, newLat, newLon);

	int i = 0;
	int j = 0;
	for(i = 0; i < 2 * nRow; i++) {
		for(j = 0; j < 2 * nCol; j++) {
			printf("%lf,%lf\n", newLat[i * 2 * nCol + j], newLon[i * 2 * nCol + j]);
		}
	}


	fclose(input);

	free(oriLat);
	free(oriLon);
	free(newLat);
	free(newLon);
	
	printf("Finished!\n");

	return 0;
}
