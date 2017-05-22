/**
 * MODISLatLon.c
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
 * NAME:	upscaleLatLonPlanar
 * DESCRIPTION:	calculate the latitude and longitude of MODIS cell centers at finner resolution based on coarser resolution cell locations, using a planer cooridnate system
 * PARAMETERS:
 * 	double * oriLat:	the latitudes of input cells at coarser resolution
 * 	double * oriLon:	the longitudes of input cells at coarser resolution
 * 	int nRow:		the number of rows of input raster
 * 	int nRol:		the number of columns of input raster
 * 	int scanSize:		the number of cells in a scan
 * 	double * newLat:	the latitudes of output cells at finer resolution (the number of rows and columns will be doubled)
 * 	double * newLon:	the longitudes of output cells at finer resolution (the number of rows and columns will be doubled)
 * Output:
 * 	double * newLat:	the latitudes of output cells at finer resolution (the number of rows and columns will be doubled)
 * 	double * newLon:	the longitudes of output cells at finer resolution (the number of rows and columns will be doubled)
 */

void upscaleLatLonPlanar(double * oriLat, double * oriLon, int nRow, int nCol, int scanSize, double * newLat, double * newLon) {

	if(0 != nRow % scanSize) {
		printf("nRows:%d is not a multiple of scanSize: %d\n", nRow, scanSize);
		exit(1);
	}

	double * step1Lat;
	double * step1Lon;

	if(NULL == (step1Lat = (double *)malloc(sizeof(double) * 2 * nRow * nCol))) {
		printf("Out of memeory for step1Lat\n");
		exit(1);
	}

	if(NULL == (step1Lon = (double *)malloc(sizeof(double) * 2 * nRow * nCol))) {
		printf("Out of memeory for step1Lon\n");
		exit(1);
	}

	int i, j;

	// First step for bilinear interpolation
	for(i = 0; i < nRow; i++) {
		for(j = 0; j < nCol; j++) {

			step1Lat[i * 2 * nCol + 2 * j] = oriLat[i * nCol + j];
			step1Lon[i * 2 * nCol + 2 * j] = oriLon[i * nCol + j];

			if(j == nCol - 1) {
				step1Lat[i * 2 * nCol + 2 * j + 1] = 1.5 * oriLat[i * nCol + j] - 0.5 * oriLat[i * nCol + j - 1];

				if(oriLon[i * nCol + j] > 90 && oriLon[i * nCol + j - 1] < -90) {
					step1Lon[i * 2 * nCol + 2 * j + 1] = 1.5 * oriLon[i * nCol + j] - 0.5 * (oriLon[i * nCol + j - 1] + 360);
				}
				else if(oriLon[i * nCol + j] < -90 && oriLon[i * nCol + j - 1] > 90) {
					step1Lon[i * 2 * nCol + 2 * j + 1] = 1.5 * (oriLon[i * nCol + j] + 360) - 0.5 * oriLon[i * nCol + j - 1];
				}
				else{
					step1Lon[i * 2 * nCol + 2 * j + 1] = 1.5 * oriLon[i * nCol + j] - 0.5 * oriLon[i * nCol + j - 1];
				}
				if(step1Lon[i * 2 * nCol + 2 * j + 1] > 180) {
					step1Lon[i * 2 * nCol + 2 * j + 1] -= 360;
				}
				else if(step1Lon[i * 2 * nCol + 2 * j + 1] < -180) {
					step1Lon[i * 2 * nCol + 2 * j + 1] += 360;
				}
			}
			else {
				step1Lat[i * 2 * nCol + 2 * j + 1] = (oriLat[i * nCol + j] + oriLat[i * nCol + j + 1]) / 2;

				if(oriLon[i * nCol + j] > 90 && oriLon[i * nCol + j + 1] < -90) {
					step1Lon[i * 2 * nCol + 2 * j + 1] = (oriLon[i * nCol + j] + oriLon[i * nCol + j + 1] + 360) / 2;
				}
				else if(oriLon[i * nCol + j] < -90 && oriLon[i * nCol + j + 1] > 90) {
					step1Lon[i * 2 * nCol + 2 * j + 1] = (oriLon[i * nCol + j] + oriLon[i * nCol + j + 1] + 360) / 2;
				}
				else {
					step1Lon[i * 2 * nCol + 2 * j + 1] = (oriLon[i * nCol + j] + oriLon[i * nCol + j + 1]) / 2;
				}
				if(step1Lon[i * 2 * nCol + 2 * j + 1] > 180) {
					step1Lon[i * 2 * nCol + 2 * j + 1] -= 360;
				}
				else if(step1Lon[i * 2 * nCol + 2 * j + 1] < -180) {
					step1Lon[i * 2 * nCol + 2 * j + 1] += 360;
				}
			}
		}
	}	

	// Second step for bilinear interpolation
	
	int k = 0;
	for(k = 0; k < nRow; k += scanSize) {

		//  Processing the first row in a scan
		i = k;
		for(j = 0; j < 2 * nCol; j++) {
			newLat[(i * 2) * 2 * nCol + j] = 1.25 * step1Lat[i * 2 * nCol + j] - 0.25 * step1Lat[(i + 1) * 2 * nCol + j];
		
			if(step1Lon[i * 2 * nCol + j] > 90 && step1Lon[(i + 1) * 2 * nCol + j] < -90) {
				newLon[(i * 2) * 2 * nCol + j] = 1.25 * step1Lon[(i + 1) * 2 * nCol + j] - 0.25 * (step1Lon[(i + 1) * 2 * nCol + j] + 360);
			}
			else if(step1Lon[i * 2 * nCol + j] < -90 && step1Lon[(i + 1) * 2 * nCol + j] > 90) {
				newLon[(i * 2) * 2 * nCol + j] = 1.25 * (step1Lon[i * 2 * nCol + j] + 360) - 0.25 * step1Lon[(i + 1) * 2 * nCol + j];
			}
			else {
				newLon[(i * 2) * 2 * nCol + j] = 1.25 * step1Lon[i * 2 * nCol + j] - 0.25 * step1Lon[(i + 1) * 2 * nCol + j];
			}
			if(newLon[(i * 2) * 2 * nCol + j] > 180) {
				newLon[(i * 2) * 2 * nCol + j] -= 360;
			}
			else if(newLon[(i * 2) * 2 * nCol + j] < -180) {
				newLon[(i * 2) * 2 * nCol + j] += 360;
			}
		}

		//  Processing intermediate rows in a scan
		for(i = k; i < k + scanSize - 1; i ++) {
			for(j = 0; j < 2 * nCol; j++) {
				newLat[(i * 2 + 1) * 2 * nCol + j] = 0.75 * step1Lat[i * 2 * nCol + j] + 0.25 * step1Lat[(i + 1) * 2 * nCol + j];
				newLat[(i * 2 + 2) * 2 * nCol + j] = 0.25 * step1Lat[i * 2 * nCol + j] + 0.75 * step1Lat[(i + 1) * 2 * nCol + j];
	
				if(step1Lon[i * 2 * nCol + j] > 90 && step1Lon[(i + 1) * 2 * nCol + j] < -90) {
					newLon[(i * 2 + 1) * 2 * nCol + j] = 0.75 * step1Lon[i * 2 * nCol + j] + 0.25 * (step1Lon[(i + 1) * 2 * nCol + j] + 360);
					newLon[(i * 2 + 2) * 2 * nCol + j] = 0.25 * step1Lon[i * 2 * nCol + j] + 0.75 * (step1Lon[(i + 1) * 2 * nCol + j] + 360);
				}
				else if(step1Lon[i * 2 * nCol + j] < -90 && step1Lon[(i + 1) * 2 * nCol + j] > 90) {
					newLon[(i * 2 + 1) * 2 * nCol + j] = 0.75 * (step1Lon[i * 2 * nCol + j] + 360) + 0.25 * step1Lon[(i + 1) * 2 * nCol + j];
					newLon[(i * 2 + 2) * 2 * nCol + j] = 0.25 * (step1Lon[i * 2 * nCol + j] + 360) + 0.75 * step1Lon[(i + 1) * 2 * nCol + j];
				}
				else{
					newLon[(i * 2 + 1) * 2 * nCol + j] = 0.75 * step1Lon[i * 2 * nCol + j] + 0.25 * step1Lon[(i + 1) * 2 * nCol + j];
					newLon[(i * 2 + 2) * 2 * nCol + j] = 0.25 * step1Lon[i * 2 * nCol + j] + 0.75 * step1Lon[(i + 1) * 2 * nCol + j];
				}

				if(newLon[(i * 2 + 1) * 2 * nCol + j] > 180) {
					newLon[(i * 2 + 1) * 2 * nCol + j] -= 360;
				}
				else if(newLon[(i * 2 + 1) * 2 * nCol + j] < -180) {
					newLon[(i * 2 + 1) * 2 * nCol + j] += 360;
				}
				if(newLon[(i * 2 + 2) * 2 * nCol + j] > 180) {
					newLon[(i * 2 + 2) * 2 * nCol + j] -= 360;
				}
				else if(newLon[(i * 2 + 2) * 2 * nCol + j] < -180) {
					newLon[(i * 2 + 2) * 2 * nCol + j] += 360;
				}
			}
		}	

		//  Processing the last row in a scan
		i = k + scanSize - 1;
		for(j = 0; j < 2 * nCol; j++) {
			newLat[(2 * i + 1) * 2 * nCol + j] = 1.25 * step1Lat[i * 2 * nCol + j] - 0.25 * step1Lat[(i - 1) * 2 * nCol + j];

			if(step1Lon[i * 2 * nCol + j] > 90 && step1Lon[(i - 1) * 2 * nCol + j] < -90) {
				newLon[(2 * i + 1) * 2 * nCol + j] = 1.25 * step1Lon[i * 2 * nCol + j] - 0.25 * (step1Lon[(i - 1) * 2 * nCol + j] + 360);
			}
			else if(step1Lon[i * 2 * nCol + j] < -90 && step1Lon[(i - 1) * 2 * nCol + j] > 90) {
				newLon[(2 * i + 1) * 2 * nCol + j] = 1.25 * (step1Lon[i * 2 * nCol + j] + 360) - 0.25 * step1Lon[(i - 1) * 2 * nCol + j];
			}
			else {
				newLon[(2 * i + 1) * 2 * nCol + j] = 1.25 * step1Lon[i * 2 * nCol + j] - 0.25 * step1Lon[(i - 1) * 2 * nCol + j];
			}
			if(newLon[(2 * i + 1) * 2 * nCol + j] > 180) {
				newLon[(2 * i + 1) * 2 * nCol + j] -= 360;
			}
			else if(newLon[(2 * i + 1) * 2 * nCol + j] < -180) {
				newLon[(2 * i + 1) * 2 * nCol + j] += 360;
			}
		}
	}
/*
	for(i = 0; i < nRow; i++) {
		for(j = 0; j < 2 * nCol; j++) {
			printf("%lf,%lf\n", step1Lat[i * 2 * nCol + j], step1Lon[i * 2 * nCol + j]);
		}
	}
*/
	free(step1Lat);
	free(step1Lon);	

	
	return;
}

/**
 * NAME:	upscaleLatLonSpherical
 * DESCRIPTION:	calculate the latitude and longitude of MODIS cell centers at finner resolution based on coarser resolution cell locations, using a planer cooridnate system
 * PARAMETERS:
 * 	double * oriLat:	the latitudes of input cells at coarser resolution
 * 	double * oriLon:	the longitudes of input cells at coarser resolution
 * 	int nRow:		the number of rows of input raster
 * 	int nRol:		the number of columns of input raster
 * 	int scanSize:		the number of cells in a scan
 * 	double * newLat:	the latitudes of output cells at finer resolution (the number of rows and columns will be doubled)
 * 	double * newLon:	the longitudes of output cells at finer resolution (the number of rows and columns will be doubled)
 * Output:
 * 	double * newLat:	the latitudes of output cells at finer resolution (the number of rows and columns will be doubled)
 * 	double * newLon:	the longitudes of output cells at finer resolution (the number of rows and columns will be doubled)
 */

void upscaleLatLonSpherical(double * oriLat, double * oriLon, int nRow, int nCol, int scanSize, double * newLat, double * newLon) {

	if(0 != nRow % scanSize) {
		printf("nRows:%d is not a multiple of scanSize: %d\n", nRow, scanSize);
		exit(1);
	}

	double * step1Lat;
	double * step1Lon;

	if(NULL == (step1Lat = (double *)malloc(sizeof(double) * 2 * nRow * nCol))) {
		printf("Out of memeory for step1Lat\n");
		exit(1);
	}

	if(NULL == (step1Lon = (double *)malloc(sizeof(double) * 2 * nRow * nCol))) {
		printf("Out of memeory for step1Lon\n");
		exit(1);
	}

	int i, j;

	double * oLat;
	double * oLon;
	if(NULL == (oLat = (double *)malloc(sizeof(double) * nRow * nCol))) {
		printf("Out of memeory for oLat\n");
		exit(1);
	}

	if(NULL == (oLon = (double *)malloc(sizeof(double) * nRow * nCol))) {
		printf("Out of memeory for oLon\n");
		exit(1);
	}


	// Convert oriLat and oriLon to radians
	for(i = 0; i < nRow; i++) {
		for(j = 0; j < nCol; j++) {
			oLat[i * nCol + j] = oriLat[i * nCol + j] * M_PI / 180; 
			oLon[i * nCol + j] = oriLon[i * nCol + j] * M_PI / 180; 
		}
	}

	double phi1, phi2, phi3, lambda1, lambda2, lambda3, bX, bY;
	double dPhi, dLambda;
	double a, b, x, y, z, f, delta;
	double f1, f2;


	// First step for bilinear interpolation
	f = 1.5;
	for(i = 0; i < nRow; i++) {
		for(j = 0; j < nCol; j++) {

			step1Lat[i * 2 * nCol + 2 * j] = oLat[i * nCol + j];
			step1Lon[i * 2 * nCol + 2 * j] = oLon[i * nCol + j];

			if(j == nCol - 1) {
	
				phi1 = oLat[i * nCol + j - 1];
				phi2 = oLat[i * nCol + j];
				lambda1 = oLon[i * nCol + j - 1];
				lambda2 = oLon[i * nCol + j];

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

				step1Lat[i * 2 * nCol + 2 * j + 1] = phi3;
				step1Lon[i * 2 * nCol + 2 * j + 1] = lambda3;
			}

			else {

				phi1 = oLat[i * nCol + j];
				phi2 = oLat[i * nCol + j + 1];
				lambda1 = oLon[i * nCol + j];
				lambda2 = oLon[i * nCol + j + 1];

				bX = cos(phi2) * cos(lambda2 - lambda1);
				bY = cos(phi2) * sin(lambda2 - lambda1);

				phi3 = atan2(sin(phi1) + sin(phi2), sqrt((cos(phi1) + bX) * (cos(phi1) + bX) + bY * bY));
				step1Lat[i * 2 * nCol + 2 * j + 1] = phi3;

				lambda3 = lambda1 + atan2(bY, cos(phi1) + bX) + 3 * M_PI;
				lambda3 = lambda3 - (int)(lambda3 / (2 * M_PI)) * 2 * M_PI - M_PI;

				step1Lon[i * 2 * nCol + 2 * j + 1] = lambda3;	
			}
		}
	}	

	// Second step for bilinear interpolation
	f = 1.25;
	f1 = 0.25;
	f2 = 0.75;

	int k = 0;
	for(k = 0; k < nRow; k += scanSize) {

		//  Processing the first row in a scan
		i = k;
		for(j = 0; j < 2 * nCol; j++) {
			phi1 = step1Lat[(i + 1) * 2 * nCol + j];
			phi2 = step1Lat[i * 2 * nCol + j];
			lambda1 = step1Lon[(i + 1) * 2 * nCol + j];
			lambda2 = step1Lon[i * 2 * nCol + j];
	
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

			newLat[(i * 2) * 2 * nCol + j] = phi3;
			newLon[(i * 2) * 2 * nCol + j] = lambda3;
		}

		//  Processing intermediate rows in a scan
		for(i = k; i < k + scanSize - 1; i ++) {
			for(j = 0; j < 2 * nCol; j++) {
				
				phi1 = step1Lat[i * 2 * nCol + j];
				phi2 = step1Lat[(i + 1) * 2 * nCol + j];
				lambda1 = step1Lon[i * 2 * nCol + j];
				lambda2 = step1Lon[(i + 1) * 2 * nCol + j];
			
				dPhi = (phi2 - phi1);
				dLambda = (lambda2 - lambda1);

				a = sin(dPhi/2) * sin(dPhi/2) + cos(phi1) * cos(phi2) * sin(dLambda/2) * sin(dLambda/2);
				delta = 2 * atan2(sqrt(a), sqrt(1-a));

				//Interpolate the first intermediate point
				a = sin((1-f1) * delta) / sin(delta);
				b = sin(f1 * delta) / sin(delta);
			
				x = a * cos(phi1) * cos(lambda1) + b * cos(phi2) * cos(lambda2);
				y = a * cos(phi1) * sin(lambda1) + b * cos(phi2) * sin(lambda2);
				z = a * sin(phi1) + b * sin(phi2);

				phi3 = atan2(z, sqrt(x * x + y * y));
				lambda3 = atan2(y, x);

				newLat[(i * 2 + 1) * 2 * nCol + j] = phi3;
				newLon[(i * 2 + 1) * 2 * nCol + j] = lambda3;

				//Interpolate the second intermediate point
				a = sin((1-f2) * delta) / sin(delta);
				b = sin(f2 * delta) / sin(delta);

				x = a * cos(phi1) * cos(lambda1) + b * cos(phi2) * cos(lambda2);
				y = a * cos(phi1) * sin(lambda1) + b * cos(phi2) * sin(lambda2);
				z = a * sin(phi1) + b * sin(phi2);
	
				phi3 = atan2(z, sqrt(x * x + y * y));
				lambda3 = atan2(y, x);

				newLat[(i * 2 + 2) * 2 * nCol + j] = phi3;
				newLon[(i * 2 + 2) * 2 * nCol + j] = lambda3;
			}
		}

		//  Processing the last row in a scan
		i = k + scanSize - 1;
		for(j = 0; j < 2 * nCol; j++) {
			phi1 = step1Lat[(i - 1) * 2 * nCol + j];
			phi2 = step1Lat[i * 2 * nCol + j];
			lambda1 = step1Lon[(i - 1) * 2 * nCol + j];
			lambda2 = step1Lon[i * 2 * nCol + j];

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

			newLat[(2 * i + 1) * 2 * nCol + j] = phi3;
			newLon[(2 * i + 1) * 2 * nCol + j] = lambda3;
		}
	}

/*
	for(i = 0; i < nRow; i++) {
		for(j = 0; j < 2 * nCol; j++) {
			printf("%lf,%lf\n", step1Lat[i * 2 * nCol + j], step1Lon[i * 2 * nCol + j]);
		}
	}
*/

	// Convert newLat and newLon to degrees
	for(i = 0; i < 2 * nRow; i++) {
		for(j = 0; j < 2 * nCol; j++) {
			newLat[i * 2 * nCol + j] = newLat[i * 2 * nCol + j] * 180 / M_PI;			
			newLon[i * 2 * nCol + j] = newLon[i * 2 * nCol + j] * 180 / M_PI;
		}
	}

	free(oLat);
	free(oLon);
	free(step1Lat);
	free(step1Lon);	

	
	return;



}

