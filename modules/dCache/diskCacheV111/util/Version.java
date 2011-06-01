package diskCacheV111.util ;

import java.io.IOException;
import java.io.InputStream;

import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import java.net.URL;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.2, 19 Nov 2005
  */


public class Version {

    private static String __specVersion = "undefined" ;
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
                String packageVersion = as.getValue("Package-Version");
                if( packageVersion != null ) {
                    __specVersion = packageVersion;
                }
            }

       }catch(IOException ee){}
    
    }
    public static String getVersion() { return __specVersion ; }
    public static String getBuildTime() { return __buildTime; }
    public static void main( String [] args ){
       System.out.println(__specVersion) ;
    }
}
