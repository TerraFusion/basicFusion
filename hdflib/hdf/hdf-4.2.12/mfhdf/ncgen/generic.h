/*********************************************************************
 *   Copyright 1993, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *   $Id: generic.h 2365 1996-03-26 22:43:35Z georgev $
 *********************************************************************/

union generic {			/* used to hold any kind of fill_value */
    double doublev;
    float floatv;
    nclong longv;
    short shortv;
    char charv;
};
