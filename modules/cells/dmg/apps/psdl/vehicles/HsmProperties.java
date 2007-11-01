package dmg.apps.psdl.vehicles ;

import java.io.* ;

public interface HsmProperties extends Serializable {

/**
  * the following contructors are required as well
  *
  *  public HsmPropertiesOSM( File trashDir , PnfsId pnfsId ) 
  *         throws IOException ;
  *  public HsmPropertiesOSM( PnfsFile dir , PnfsFile file ) 
  *         throws IOException ;
  */
   public String getHsmKey() ;
   public String getHsmInfo() ;
   public boolean isFile() ;
}
 
