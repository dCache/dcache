package dmg.cells.applets.spy ;

import dmg.util.CommandRequestable;

public class SpyCommandRequest implements CommandRequestable {
   private static final long serialVersionUID = 7012972757477500153L;
   private String    _command ;
   private Object [] _params ;
   public SpyCommandRequest( String command , Object key , Object value ){
       _command   = command ;
       _params    = new Object[2] ;
       _params[0] = key ;
       _params[1] = value ;
   }
   @Override
   public String getRequestCommand(){ return _command ; }
   @Override
   public int    getArgc(){ return 2 ; }
   @Override
   public Object getArgv(int position){ return _params[position] ; }

   @Override
   public String toString() { return _command + " " + _params[0] + " " + _params[1]; }
}
