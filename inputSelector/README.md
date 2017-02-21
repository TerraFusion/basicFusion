File formats and encodings:

* Orbit: 1000 2000-02-25T00:25:07Z 2000-02-25T02:04:00Z
  * seq: NNNN 
  * start:    YYYY-MM-DD_TTTTTTTTT
  * end:                           (similar)

* MODIS: MOD0PPP.AYYYYDDD.HHMM.VVV.YYYYDDDHHMMSS.hdf
 * start:         YYYYDDD.HHMM.(?VVV)
 * end:                            YYYYDDDHHMMSS

* MISR: MISR_AM1_RP_GM_Pmmm_Onnnnnn_cc_Fff_vvvv.hdf
 * orbit:                    nnnnnn
 * start:   = time(orbit, nnnnnn)
 * end:     = time(orbit, nnnnnn+1) ????

 * ASTER: AST_L1T_003DDMMYYYYhhmmss_YYYYMMDD.hdf
 
 * start:            ddmmyyyyhhmmss
 * end:      ????

 * CERES: CER_SSF_Terra-FM1-MODIS_Edition4A_400403.2007070317
 * start:                                          YYYYMMDDHH ??

 * MOPPIT: MOP01-20070703-L1V3.50.0.he5
 * start:        YYYYMMDD