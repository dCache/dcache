package dmg.security.digest ;

import java.security.NoSuchAlgorithmException;

public class Md5ext extends GenDigest {
    public Md5ext()
    throws NoSuchAlgorithmException{
        super( "MD5" ) ;
    }
}
