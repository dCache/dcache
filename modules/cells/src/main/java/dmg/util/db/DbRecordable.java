package dmg.util.db ;

import java.util.Enumeration;
public interface DbRecordable extends DbLockable {
   public void setAttribute( String name , String attribute ) ;

   public void setAttribute( String name , String [] attribute ) ;

   public Object getAttribute( String name ) ;

   public Enumeration<String> getAttributes();

   public void remove() ;

}
