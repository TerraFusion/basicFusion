/**
 * ASTERLatLon.c
 * Authors: Yizhao Gao <ygao29@illinois.edu>
 * Date: {05/22/2017}
 */

#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#ifndef M_PI
#    define M_PI 3.14159265358979323846
#endif


/**
 * NAME:	asterLatLonPlanar
 * DESCRIPTION:	calculate the latitude and longitude of ASTER cell centers at finner resolution using a planar cooridnate system
 * PARAMETERS:
 * 	double * inLat:		the input 11 * 11 latitudes
 * 	double * inLon:		the input 11 * 11 longitudes
 * 	double * cLat:		the output latitudes of cells
 * 	double * cLon:		the ouput longitudes of cells
 * 	int nRow:		the number of rows of output raster
 * 	int nCol:		the number of columns of output raster
 * Output:
 * 	double * cLat:		the output latitudes of cells
 * 	double * cLon:		the ouput longitudes of cells
 */

void asterLatLonPlanar(double * inLat, double * inLon, double * cLat, double * cLon, int nRow, int nCol) {

	double iLat[121];
	double iLon[121];
	
	for(int i = 0; i < 121; i++) {
		iLat[i] = inLat[i];
		iLon[i] = inLon[i];
	}

//Longitude +360 if the image crosses 180/-180 lon
	int above90 = 0;
	int belowM90 = 0;
	int adjust = 0;

	for(int i = 0; i < 121; i++) {
		if(iLon[i] > 90) {
			above90 = 1;
		}
		else if(iLon[i] < -90) {
			belowM90 = 1;
		}
	}

	if(above90 == 1 && belowM90 == 1) {
		adjust = 1;
	}

	if(adjust == 1) {
//		printf("Adjust\n");
		for(int i = 0; i < 121; i++) {
			if(iLon[i] < -90) {
				iLon[i] += 360;
			}
		}
	}

//Bilinear interpolation
	double dL = ((double)nRow - 1) / 10;
	double dS = ((double)nCol - 1) / 10;

	int r;
	int c;

	double lr, lr1, sc, sc1;
	double x, y;


	double * step1Lat;
	double * step1Lon;

	if(NULL == (step1Lat = (double *)(malloc(sizeof(double) * 11 * nCol)))) {
		printf("Out of memeory for step1Lat\n");
		exit(1);	
	}

	if(NULL == (step1Lon = (double *)(malloc(sizeof(double) * 11 * nCol)))) {
		printf("Out of memeory for step1Lon\n");
		exit(1);	
	}


//Step 1
	for(int s = 0; s < nCol - 1; s++) {
		
		c = s / dS;
		sc = c * dS;
		sc1 = (c + 1) * dS;

		x = (s - sc) / dS;

		for(int l = 0; l < 11; l++) {
			
			step1Lat[l * nCol + s] = (1 - x) * iLat[l * 11 + c] + x * iLat[l * 11 + c + 1];
			step1Lon[l * nCol + s] = (1 - x) * iLon[l * 11 + c] + x * iLon[l * 11 + c + 1];
			
		}
	}

	for(int l = 0; l < 11; l++) {
		step1Lat[l * nCol + nCol - 1] = iLat[l * 11 + 10];
		step1Lon[l * nCol + nCol - 1] = iLon[l * 11 + 10];
	}

//Step 2
	for(int l = 0; l < nRow - 1; l++) {
		
		r = l / dL;
		lr = r * dL;
		lr1 = (r + 1) * dL;

		y = (l - lr) / dL;

		for(int s = 0; s < nCol; s++) {
			
			cLat[l * nCol+ s] = (1 - y) * step1Lat[r * nCol + s] + y * step1Lat[(r + 1) * nCol + s];
			cLon[l * nCol+ s] = (1 - y) * step1Lon[r * nCol + s] + y * step1Lon[(r + 1) * nCol + s];
		}
	}

	for(int s = 0; s < nCol; s++) {
	
		cLat[(nRow - 1) * nCol + s] = step1Lat[10 * nCol + s];
		cLon[(nRow - 1) * nCol + s] = step1Lon[10 * nCol + s];
	}

	free(step1Lat);
	free(step1Lon);

//Adjust the longitude back if necessary
	if(adjust == 1) {
		for(int i = 0; i < nCol * nRow; i++) {
			if(cLon[i] > 180) {
				cLon[i] -= 360;
			}
		}	
	}
}

/**
 * NAME:	asterLatLonPlanarOLD
 * DESCRIPTION:	OLD and not optimized implementation of asterLatLonPlanar 
 * PARAMETERS:
 * 	double * inLat:		the input 11 * 11 latitudes
 * 	double * inLon:		the input 11 * 11 longitudes
 * 	double * cLat:		the output latitudes of cells
 * 	double * cLon:		the ouput longitudes of cells
 * 	int nRow:		the number of rows of output raster
 * 	int nCol:		the number of columns of output raster
 * Output:
 * 	double * cLat:		the output latitudes of cells
 * 	double * cLon:		the ouput longitudes of cells
 */

void asterLatLonPlanarOLD(double * inLat, double * inLon, double * cLat, double * cLon, int nRow, int nCol) {
	
	double iLat[121];
	double iLon[121];
	
	for(int i = 0; i < 121; i++) {
		iLat[i] = inLat[i];
		iLon[i] = inLon[i];
	}

//Longitude +360 if the image crosses 180/-180 lon
	int above90 = 0;
	int belowM90 = 0;
	int adjust = 0;

	for(int i = 0; i < 121; i++) {
		if(iLon[i] > 90) {
			above90 = 1;
		}
		else if(iLon[i] < -90) {
			belowM90 = 1;
		}
	}

	if(above90 == 1 && belowM90 == 1) {
		adjust = 1;
	}

	if(adjust == 1) {
//		printf("Adjust\n");
		for(int i = 0; i < 121; i++) {
			if(iLon[i] < -90) {
				iLon[i] += 360;
			}
		}
	}

//Bilinear interpolation
	double dL = ((double)nRow - 1) / 10;
	double dS = ((double)nCol - 1) / 10;

	int r;
	int c;

	double lr, lr1, sc, sc1;
	double x, y;

	for(int l = 0; l < nRow; l++) {
		
		for(int s = 0; s < nCol; s++) {
		
			r = l / dL;
			c = s / dS;
			
			lr = r * dL;
			lr1 = (r + 1) * dL;

			sc = c * dS;
			sc1 = (c + 1) * dS;

			x = (s - sc) / dS;
			y = (l - lr) / dL;

			if(r == 10) {
				if(c == 10) {
					cLat[l * nCol + s] = iLat[120];
					cLon[l * nCol + s] = iLon[120];
				}
				else {
					cLat[l * nCol + s] = (1 - x) * iLat[r * 11 + c] + x * iLat[r * 11 + c + 1]; 
					cLon[l * nCol + s] = (1 - x) * iLon[r * 11 + c] + x * iLon[r * 11 + c + 1];
				}
			}
			else {
				if(c == 10) {
					cLat[l * nCol + s] = (1 - y) * iLat[r * 11 + c] + y * iLat[r * 11 + c + 11];
					cLon[l * nCol + s] = (1 - y) * iLon[r * 11 + c] + y * iLon[r * 11 + c + 11];
				}
				else {
					cLat[l * nCol + s] = (1 - x) * (1 - y) * iLat[r * 11 + c] + x * (1 - y) * iLat[r * 11 + c + 1] + (1 - x) * y * iLat[r * 11 + c + 11] + x * y * iLat[r * 11 + c + 12]; 
					cLon[l * nCol + s] = (1 - x) * (1 - y) * iLon[r * 11 + c] + x * (1 - y) * iLon[r * 11 + c + 1] + (1 - x) * y * iLon[r * 11 + c + 11] + x * y * iLon[r * 11 + c + 12];
				}
			}	
		}
	}
	
//Adjust the longitude back if necessary
	if(adjust == 1) {
		for(int i = 0; i < nCol * nRow; i++) {
			if(cLon[i] > 180) {
				cLon[i] -= 360;
			}
		}	
	}

}

/**
 * NAME:	asterLatLonSpherical
 * DESCRIPTION:	calculate the latitude and longitude of ASTER cell centers at finner resolution using a Spherical cooridnate system
 * PARAMETERS:
 * 	double * inLat:		the input 11 * 11 latitudes
 * 	double * inLon:		the input 11 * 11 longitudes
 * 	double * cLat:		the output latitudes of cells
 * 	double * cLon:		the ouput longitudes of cells
 * 	int nRow:		the number of rows of output raster
 * 	int nCol:		the number of columns of output raster
 * Output:
 * 	double * cLat:		the output latitudes of cells
 * 	double * cLon:		the ouput longitudes of cells
 */

void asterLatLonSpherical(double * inLat, double * inLon, double * cLat, double * cLon, int nRow, int nCol) {

//Bilinear interpolation
	double dL = ((double)nRow - 1) / 10;
	double dS = ((double)nCol - 1) / 10;

	int r;
	int c;

	double lr, lr1, sc, sc1;

	double phi1, phi2, phi3, lambda1, lambda2, lambda3, bX, bY;
	double dPhi, dLambda;
	double a, b, x, y, z, f, delta;


	double * step1Lat;
	double * step1Lon;

	if(NULL == (step1Lat = (double *)(malloc(sizeof(double) * 11 * nCol)))) {
		printf("Out of memeory for step1Lat\n");
		exit(1);	
	}

	if(NULL == (step1Lon = (double *)(malloc(sizeof(double) * 11 * nCol)))) {
		printf("Out of memeory for step1Lon\n");
		exit(1);	
	}
	
	double iLat[121];
	double iLon[121];
	
	// Convert oriLat and oriLon to radians
	for(int i = 0; i < 11 * 11; i++) {
		iLat[i] = inLat[i] * M_PI / 180; 
		iLon[i] = inLon[i] * M_PI / 180; 
	}

//Step 1
	for(int s = 0; s < nCol - 1; s++) {
		
		c = s / dS;
		sc = c * dS;
		sc1 = (c + 1) * dS;

		f = (s - sc) / dS;

		for(int l = 0; l < 11; l++) {
			
			phi1 = iLat[l * 11 + c];
			phi2 = iLat[l * 11 + c + 1];
			lambda1 = iLon[l * 11 + c];
			lambda2 = iLon[l * 11 + c + 1];

			dPhi = (phi2 - phi1);
			dLambda = (lambda2 - lambda1);

			a = sin(dPhi/2) * sin(dPhi/2) + cos(phi1) * cos(phi2) * sin(dLambda/2) * sin(dLambda/2);
			delta = 2 * atan2(sqrt(a), sqrt(1-a));

			a = sin((1-f) * delta) / sin(delta);
			b = sin(f * delta) / sin(delta);

			x = a * cos(phi1) * cos(lambda1) + b * cos(phi2) * cos(lambda2);
			y = a * cos(phi1) * sin(lambda1) + b * cos(phi2) * sin(lambda2);
			z = a * sin(phi1) + b * sin(phi2);

			phi3 = atan2(z, sqrt(x * x + y * y));
			lambda3 = atan2(y, x);

			step1Lat[l * nCol + s] = phi3;
			step1Lon[l * nCol + s] = lambda3;

		}
	}

	for(int l = 0; l < 11; l++) {
		step1Lat[l * nCol + nCol - 1] = iLat[l * 11 + 10];
		step1Lon[l * nCol + nCol - 1] = iLon[l * 11 + 10];
	}

//Step 2
	for(int l = 0; l < nRow - 1; l++) {
		
		r = l / dL;
		lr = r * dL;
		lr1 = (r + 1) * dL;

		f = (l - lr) / dL;

		for(int s = 0; s < nCol; s++) {
			
			cLat[l * nCol+ s] = (1 - y) * step1Lat[r * nCol + s] + y * step1Lat[(r + 1) * nCol + s];
			cLon[l * nCol+ s] = (1 - y) * step1Lon[r * nCol + s] + y * step1Lon[(r + 1) * nCol + s];

			phi1 = step1Lat[r * nCol + s];
			phi2 = step1Lat[(r + 1) * nCol + s];
			lambda1 = step1Lon[r * nCol + s];
			lambda2 = step1Lon[(r + 1) * nCol + s];

			dPhi = (phi2 - phi1);
			dLambda = (lambda2 - lambda1);

			a = sin(dPhi/2) * sin(dPhi/2) + cos(phi1) * cos(phi2) * sin(dLambda/2) * sin(dLambda/2);
			delta = 2 * atan2(sqrt(a), sqrt(1-a));

			a = sin((1-f) * delta) / sin(delta);
			b = sin(f * delta) / sin(delta);

			x = a * cos(phi1) * cos(lambda1) + b * cos(phi2) * cos(lambda2);
			y = a * cos(phi1) * sin(lambda1) + b * cos(phi2) * sin(lambda2);
			z = a * sin(phi1) + b * sin(phi2);

			phi3 = atan2(z, sqrt(x * x + y * y));
			lambda3 = atan2(y, x);

			cLat[l * nCol+ s] = phi3;
			cLon[l * nCol+ s] = lambda3;


		}
	}

	for(int s = 0; s < nCol; s++) {
	
		cLat[(nRow - 1) * nCol + s] = step1Lat[10 * nCol + s];
		cLon[(nRow - 1) * nCol + s] = step1Lon[10 * nCol + s];
	}

	free(step1Lat);
	free(step1Lon);
	
	// Convert newLat and newLon to degrees
	for(int i = 0; i < nRow * nCol; i++) {
		cLat[i] = cLat[i] * 180 / M_PI;			
		cLon[i] = cLon[i] * 180 / M_PI;
	}


}
