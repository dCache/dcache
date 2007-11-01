package org.dcache.tests.util;

import org.junit.Before;
import org.junit.Test;

import diskCacheV111.util.Checksum;
import diskCacheV111.util.Adler32;

public class ChecksumTestCase extends junit.framework.TestCase {
	
	private final byte[] _input = new byte[] {0,0,0,0,0,0,0,0,0,0};
		
	@Test 
	public void testMessageDigestConstructor() throws Exception {
			
		Adler32 adler;

		adler = new Adler32();
		adler.update( _input );
		Checksum c1 = new Checksum( 1 , adler.digest() );
		
		adler = new Adler32();
		Checksum c2 = new Checksum( adler );
		c2.getMessageDigest().update( _input );
	     	
		assertEquals( c1 , c2 );			
		
	}

	
    public static void main(String args[]) {
        org.junit.runner.JUnitCore.main("org.dcache.tests.util.ChecksumTestCase");
      }
}

