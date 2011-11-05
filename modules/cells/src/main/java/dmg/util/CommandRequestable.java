package dmg.util ;

import java.io.Serializable ;

public interface CommandRequestable  extends Serializable {

   static final long serialVersionUID = -702524576770448596L;

   public String getRequestCommand() ;
   public int    getArgc() ;
   public Object getArgv(int position) ;

}
