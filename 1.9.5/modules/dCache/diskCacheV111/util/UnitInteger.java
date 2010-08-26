package diskCacheV111.util ;

public class UnitInteger {
    private long _value = 0 ;
    public UnitInteger( long value ){
       _value = value ;
    }
    public UnitInteger( String stringRep ){
       _value = parseUnitLong( stringRep ) ;
    }
    public static long parseUnitLong( String stringRep ){
       if( stringRep.length() < 1 )
           throw new 
           IllegalArgumentException("Empty String") ;
       String num = null ;
       long   multi = 0 ;
       switch( stringRep.charAt(stringRep.length()-1) ){

          case 'k' :
          case 'K' :
             multi = 1024 ;
             num   = stringRep.substring(0,stringRep.length()-1) ;
             break ;
          case 'm' :
          case 'M' :
             multi = 1024 * 1024 ;
             num   = stringRep.substring(0,stringRep.length()-1) ;
             break ;
          case 'g' :
          case 'G' :
             multi = 1024 * 1024 * 1024 ;
             num   = stringRep.substring(0,stringRep.length()-1) ;
             break ;
          default :
             multi = 1 ;
             num   = stringRep ;
       }
       return  Long.parseLong(num) * multi ;
    }
    public String toString(){ return ""+_value ; }
    public String toUnitString(){ 
       return toUnitString( _value ) ;
    }
    public static String toUnitString( long value ){ 
       long tmp ;
       long g = 1024 * 1024 * 1024 ;
       long m = 1024 * 1024 ;
       long k = 1024 ;
       if( ( ( tmp = ( value / g ) ) > 0 ) && ( ( value % g ) == 0 ) )
          return ""+tmp+"G" ;
       if( ( ( tmp = ( value / m ) ) > 0 ) && ( ( value % m ) == 0 ) )
          return ""+tmp+"M" ;
       if( ( ( tmp = ( value / k ) ) > 0 ) && ( ( value % k ) == 0 ) )
          return ""+tmp+"K" ;
       return ""+value ;
    }
    public long longValue(){ return _value ; }
    public static void main( String [] args ){
       for( int i= 0 ; i < args.length ; i++ ){
           UnitInteger ui = new UnitInteger(args[i]) ;
           System.out.println( args[i]+" -> "+ui.toUnitString()+ "  ("+ui+")" );
       }
       System.exit(0);
    }
}

