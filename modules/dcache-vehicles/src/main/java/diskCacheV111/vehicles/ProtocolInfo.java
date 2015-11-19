// $Id: ProtocolInfo.java,v 1.5 2006-05-12 20:47:12 tigran Exp $

package diskCacheV111.vehicles ;

import java.io.Serializable;

/**
  * The implementions of ProtocolInfo travel from the
  * door to the mover. The actual mover class is
  * determined by the return values of the interface
  * methods as follows :
  *
  *  <pre> &lt;Protocol&gt;Protocol_&lt;MajorVersion&gt;</pre>
  *
  * The actual mover class needs to cast the interface to
  * the original class to get the necessary information
  * about the client. ( e.g. hostname, portnumber a.s.o)
  */
public interface ProtocolInfo extends Serializable {

    String getProtocol() ;
    int    getMinorVersion() ;
    int    getMajorVersion() ;
    String getVersionString() ;
}
