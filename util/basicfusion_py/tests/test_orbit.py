import bfutils.orbit as orbit
import pytest
import os

class TestOrbitStart(object):
    def test_lower( self ):
        assert orbit.orbit_start( 1000 ) == '20000225002507'
    def test_upper( self ):    
        assert orbit.orbit_start( 85302 ) == '20151231234020'
        
    def test_too_low( self ):
        with pytest.raises( ValueError ):
            orbit.orbit_start( 0 )
    def test_too_high( self ):    
        with pytest.raises( ValueError ):
            orbit.orbit_start( 99999999999 )
                
       
class TestMakeRunDir(object):
    def test_limits( self, tmpdir ):
        numiter = 10
        for i in xrange( numiter ):
            orbit.makeRunDir( str(tmpdir) )

        for i in xrange( numiter ):
            assert os.path.isdir( os.path.join( str(tmpdir), 'run{}'.format(i)))
            
    def test_not_str( self ):
        with pytest.raises( TypeError ):
            orbit.makeRunDir( 5 )


class Test_findProcPartition(object):
    def test_one( self ):
        processBin = orbit.findProcPartition( 10, 10 )
        assert len(processBin) == 10
        for i in processBin:
            assert i == 1

    def test_two( self ):
        processBin = orbit.findProcPartition( 10, 13 )
        assert len(processBin) == 10

        count = 0
        for i in processBin:
            count += i
            assert i == 1 or i == 2

        assert count == 13


class Test_isBFfile(object):
    def test_type_err(self):
        with pytest.raises( TypeError ):
            orbit.isBFfile( {} )

    def test_proper(self):
        arg = [ 'MOP01-20070703-L1V3.50.0.he5', \
                'CER_SSF_Terra-FM1-MODIS_Edition4A_400403.2007070316', \
                'CER_SSF_Terra-FM1-MODIS_Edition4A_400403.2007070317', \
                'MOD021KM.A2007184.1610.006.2014231113627.hdf', \
                'MOD02HKM.A2007184.1625.006.2014230150204.hdf', \
                'MOD02QKM.A2007184.1625.006.2014230073105.hdf', \
                'MOD03.A2007184.1625.006.2012239144215.hdf', \
                'AST_L1T_00307032007162020_20150520034221_121033.hdf', \
                '/projects/TDataFus/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_AA_F03_0024.hdf', \
                '/projects/TDataFus/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_AF_F03_0024.hdf', \
                '/projects/TDataFus/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_AN_F03_0024.hdf', \
                '/projects/TDataFus/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_BA_F03_0024.hdf', \
                '/projects/TDataFus/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_BF_F03_0024.hdf', \
                '/projects/TDataFus/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_CA_F03_0024.hdf', \
                '/projects/TDataFus/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_CF_F03_0024.hdf', \
                '/projects/TDataFus/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_DA_F03_0024.hdf', \
                '/projects/TDataFus/oneorbit/MISR/MISR_AM1_GRP_ELLIPSOID_GM_P022_O040110_DF_F03_0024.hdf', \
                '/projects/TDataFus/oneorbit/MISR/AGP/MISR_AM1_AGP_P022_F01_24.hdf', \
                '/projects/TDataFus/oneorbit/MISR/MISR_AM1_GP_GMP_P022_O040110_F03_0013.hdf', \
                '/projects/TDataFus/oneorbit/MISR/AGP_HR/MISR_HRLL_P022.hdf', \
                'none'
              ]

        correct = [ 0, 1, 1, 2, 2, 2, 2, 3, 4, 4, 4, 4, 4, 4, 4, 4, 4, 5, 6, 7, -1 ]

        ret_val = orbit.isBFfile( arg )
        assert ret_val == correct


