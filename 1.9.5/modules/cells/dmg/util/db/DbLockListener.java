package dmg.util.db ;

import java.util.* ;

public interface DbLockListener {

      public void readLockGranted() ; 
      public void writeLockGranted() ;
      public void readLockReleased() ;
      public void writeLockReleased() ;

}
