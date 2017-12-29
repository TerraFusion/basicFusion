import bfutils.orbit as orbit
import pytest
import os

class TestOrbitStart:
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
                
       
class TestMakeRunDir:
    def test_limits( self, tmpdir ):
        numiter = 10
        for i in xrange( numiter ):
            orbit.makeRunDir( str(tmpdir) )

        for i in xrange( numiter ):
            assert os.path.isdir( os.path.join( str(tmpdir), 'run{}'.format(i)))
            
    def test_not_str( self ):
        with pytest.raises( TypeError ):
            orbit.makeRunDir( 5 )


class Test_findProcPartition:
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


