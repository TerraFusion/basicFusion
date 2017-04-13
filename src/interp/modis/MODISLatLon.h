#ifndef MLLH
#define MLLH
void upscaleLatLonPlanar(double * oriLat, double * oriLon, int nRow, int nCol, int scanSize, double * newLat, double * newLon);
void upscaleLatLonSpherical(double * oriLat, double * oriLon, int nRow, int nCol, int scanSize, double * newLat, double * newLon);
#endif
