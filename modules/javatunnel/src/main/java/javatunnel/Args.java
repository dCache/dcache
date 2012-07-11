package javatunnel;

import java.util.* ;

public class Args implements java.io.Serializable {
   private Hashtable _optHash = new Hashtable() ;
   private Vector    _optv    = new Vector() ;
   private Vector    _argv    = new Vector() ;
   private String    _oneChar = null ;
   public Args( String args ) {
      scanLine( args ) ;
   }
   public Args( String [] args ) {
      StringBuffer sb = new StringBuffer() ;
      for( int i = 0 ; i < args.length ; i++ )
         sb.append(args[i]).append(" ");

      scanLine( sb.toString() ) ;
   }
   Args( Args in ){
     _argv = (Vector)in._argv.clone() ;
     _optv = (Vector)in._optv.clone() ;
     _optHash = (Hashtable)in._optHash.clone() ;
     _line = in._line ;
   }
   public boolean isOneCharOption( char c ){
     return _oneChar.indexOf(c) > -1 ;
   }
   public int argc(){ return _argv.size() ; }
   public int optc(){ return _optv.size() ; }
   public String getOpt( String optName ){ return (String)_optHash.get( optName ) ; }
   public String argv( int i ){

       try{
         return _argv.elementAt(i).toString() ;
       }catch( ArrayIndexOutOfBoundsException e ){
         return null ;
       }

   }
   public String optv( int i ){

      try{
        return (String)_optv.elementAt(i) ;
      }catch( ArrayIndexOutOfBoundsException e ){
        return null ;
      }

   }
   public void shift(){

      try{
        _argv.removeElementAt(0);
      }catch( ArrayIndexOutOfBoundsException e ){}
      return ;

   }
   public Dictionary  options() { return _optHash ; }

   public String toString(){ return _line ; }
   public String getInfo(){
      StringBuffer sb = new StringBuffer() ;

      sb.append( "Positional :\n" );
      for( int i= 0 ; i < _argv.size() ; i++ ){
         sb.append(i).append(" -> ").append(_argv.elementAt(i)).append("\n") ;
      }
      sb.append( "Options :\n" );
      for( int i= 0 ; i < _optv.size() ; i++ ){
         String key = (String)_optv.elementAt(i) ;
         String val = (String)_optHash.get(key) ;
         sb.append(key) ;
         if( val != null )
            sb.append( " -> " ).append(val) ;
         sb.append("\n") ;
      }

      return sb.toString() ;
   }
   private static final int IDLE          = 0 ;
   private static final int PLAIN_STRING  = 1 ;
   private static final int QUOTED_STRING = 2 ;
   private static final int OPT_KEY       = 3 ;
   private static final int OPT_VALUE     = 4 ;
   private static final int OPT_QUOTED    = 5 ;
   private static final int OPT_PLAIN     = 6 ;

   private void undo( char r ){ _res = r ; _undo = true ; }

   private boolean _undo    = false ;
   private char    _res     = 0 ;
   private int     _current = 0 ;
   private String  _line    = null ;
   private char nextChar() {
      if( _undo ){ _undo = false  ; return _res ; }
      else
         return _current >= _line.length() ?
                END_OF_INFO :
                _line.charAt(_current++) ;

   }

   private final static char   END_OF_INFO = (char)-1 ;
   private void scanLine( String line ){
      _line = line ;
      int  state = IDLE ;
      char c ;
      StringBuffer key = null , value = null ;
      StringBuffer oneChar = new StringBuffer() ;
      do{
         c = nextChar() ;
         switch( state ){
            case IDLE :
               if( ( c == END_OF_INFO ) || ( c == ' ' ) ){
                  // nothing to do
               }else if( c == '"' ){
                  state = QUOTED_STRING ;
                  value = new StringBuffer() ;
               }else if( c == '-' ){
                  state = OPT_KEY ;
                  key   = new StringBuffer() ;
               }else{
                  value = new StringBuffer() ;
                  value.append(c);
                  state = PLAIN_STRING ;
               }
            break ;
            case PLAIN_STRING :
               if( ( c == END_OF_INFO ) || ( c == ' ' ) ){
                  _argv.addElement( value.toString() ) ;
                  state = IDLE ;
               }else{
                  value.append(c) ;
               }
            break ;
            case QUOTED_STRING :
               if( ( c == END_OF_INFO ) ||
                   ( c == '"'         )    ){
                  _argv.addElement( value.toString() ) ;
                  state = IDLE ;
               }else{
                  value.append(c) ;
               }
            break ;
            case OPT_KEY :
               if( ( c == END_OF_INFO ) || ( c == ' ' ) ){
                  if( key.length() != 0 ){
                     _optv.addElement(key.toString()) ;
                     _optHash.put( key.toString() , "" ) ;
                     oneChar.append(key.toString());
                  }
                  state = IDLE ;
               }else if( c == '=' ){
                  value = new StringBuffer() ;
                  state = OPT_VALUE ;
               }else{
                  key.append(c) ;
               }
            break ;
            case OPT_VALUE :
               if( ( c == END_OF_INFO ) || ( c == ' ' ) ){
                  if( key.length() != 0 ){
                     _optv.addElement(key.toString()) ;
                     _optHash.put( key.toString() , "" ) ;
                  }
                  state = IDLE ;
               }else if( c == '"' ){
                  value = new StringBuffer() ;
                  state = OPT_QUOTED ;
               }else{
                  state = OPT_PLAIN ;
                  value = new StringBuffer() ;
                  value.append(c) ;
               }
            break ;
            case OPT_QUOTED :
               if( ( c == END_OF_INFO ) || ( c == '"' ) ){
                  _optv.addElement( key.toString() ) ;
                  _optHash.put( key.toString() , value.toString() ) ;
                  state =IDLE ;
               }else{
                  value.append(c) ;
               }
            break ;
            case OPT_PLAIN :
               if( ( c == END_OF_INFO ) || ( c == ' ' ) ){
                  _optv.addElement( key.toString() ) ;
                  _optHash.put( key.toString() , value.toString() ) ;
                  state =IDLE ;
               }else{
                  value.append(c) ;
               }
            break ;


         }
      }while( c != END_OF_INFO ) ;
      _oneChar = oneChar.toString() ;

   }
   public static void main( String [] args )
   {
      if( args.length < 1 ){
         System.err.println( "Usage : ... <parseString>" ) ;
         System.exit(4);
      }
      Args lineArgs = null ;
      if( args.length == 1 )
         lineArgs = new Args( args[0] ) ;
      else
         lineArgs = new Args( args );
      System.out.print( lineArgs.getInfo() ) ;
      System.out.println( "pvr="+lineArgs.getOpt( "pvr" ) ) ;

   }
}
