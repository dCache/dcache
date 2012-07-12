/*
 * Handler.java
 *
 * Created on October 17, 2003, 11:53 AM
 */

package dmg.cells.nucleus.protocols.context;

import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.lang.reflect.Constructor;

/**
 *
 * @author  timur
 */
public class Handler extends URLStreamHandler{
    
    @Override
    protected URLConnection openConnection(URL u) {
        try {
            ClassLoader threadLoader = Thread.currentThread().getContextClassLoader();
            if (threadLoader != null) {
                Class cls = threadLoader.loadClass(
                "dmg.cells.nucleus.CellUrl.DomainUrlConnection");
                Constructor constr=cls.getConstructor(new Class[]
                {URL.class,String.class});
                URLConnection connection =
                (URLConnection) constr.newInstance(new Object[]
                {u,"context"});
                return connection;
                
                
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return null;
    }
    
}
