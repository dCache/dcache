package diskCacheV111.util ;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.2, 19 Nov 2005
  */


public class Version {

    private static String __version = "undefined" ;
    private static String __buildTime = "undefined" ;

    static {
       try{
            ProtectionDomain pd = Version.class.getProtectionDomain();
            CodeSource cs = pd.getCodeSource();
            URL u = cs.getLocation();

            InputStream is = u.openStream();
            JarInputStream jis = new JarInputStream(is);
            Manifest m = jis.getManifest();

            if (m != null) {
                Attributes as = m.getMainAttributes();
                String buildTime = as.getValue("Build-Time");
                if( buildTime != null ) {
                    __buildTime = buildTime;
                }
                String version = as.getValue("Implementation-Version");
                if( version != null ) {
                    __version = version;
                }
            }

       }catch(IOException ee){}

    }
    public static String getVersion() { return __version ; }
    public static String getBuildTime() { return __buildTime; }
    public static void main( String [] args ){
       System.out.println(__version) ;
    }
}
