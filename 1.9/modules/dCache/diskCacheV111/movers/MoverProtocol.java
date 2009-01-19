// $Id: MoverProtocol.java,v 1.4 2003-05-27 14:47:34 cvs Exp $

package diskCacheV111.movers ;
import  diskCacheV111.vehicles.* ;
import  diskCacheV111.util.PnfsId ;
import  diskCacheV111.repository.SpaceMonitor ;

import  dmg.cells.nucleus.* ;
import  java.io.* ;

public interface MoverProtocol {
   public static final int READ   =  1 ;
   public static final int WRITE  =  2 ;
   //  
   // <init>( CellAdapter cell ) ;
   //
   public void runIO(
                  RandomAccessFile diskFile ,
                  ProtocolInfo protocol ,
                  StorageInfo  storage ,
                  PnfsId       pnfsId  ,
                  SpaceMonitor spaceMonitor , 
                  int          access ) 
                  
          throws Exception ;
   
   public void setAttribute( String name , Object attribute ) ;
   public Object getAttribute( String name ) ;
        
   public long getBytesTransferred() ;
   public long getTransferTime() ;
   public long getLastTransferred() ;

   public boolean wasChanged() ;
}
