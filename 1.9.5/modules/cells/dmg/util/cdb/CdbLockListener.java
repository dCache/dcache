package dmg.util.cdb ;

import java.util.* ;

public interface CdbLockListener {

      public void readLockGranted() ; 
      public void writeLockGranted() ;
      public void readLockReleased() ;
      public void writeLockReleased() ;
      public void writeLockAborted() ;

}
