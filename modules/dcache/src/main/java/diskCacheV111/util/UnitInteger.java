package diskCacheV111.util ;

public class UnitInteger {

    private static final String INFINITY = "Infinity";
    private static final long TEBI = (1L << 40);
    private static final long GIBI = (1L << 30);
    private static final long MEBI = (1L << 20);
    private static final long KIBI = (1L << 10);

    private long _value;
    public UnitInteger( long value ){
       _value = value ;
    }
    public UnitInteger( String stringRep ){
       _value = parseUnitLong( stringRep ) ;
    }
    public static long parseUnitLong( String stringRep ){
       if( stringRep.length() < 1 ) {
           throw new
                   IllegalArgumentException("Empty String");
       }

       if (stringRep.equals(INFINITY)) {
           return Long.MAX_VALUE;
       }

       String num;
       long   multi;
       switch( stringRep.charAt(stringRep.length()-1) ){

          case 'k' :
          case 'K' :
             multi = KIBI;
             num   = stringRep.substring(0,stringRep.length()-1) ;
             break ;
          case 'm' :
          case 'M' :
             multi = MEBI;
             num   = stringRep.substring(0,stringRep.length()-1) ;
             break ;
          case 'g' :
          case 'G' :
             multi = GIBI;
             num   = stringRep.substring(0,stringRep.length()-1) ;
             break ;
          case 't' :
          case 'T' :
             multi = TEBI;
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
       long tmp;
       if (value == Long.MAX_VALUE) {
           return INFINITY;
       }
       if( ( ( tmp = ( value / TEBI ) ) > 0 ) && ( ( value % TEBI ) == 0 ) ) {
           return Long.toString(tmp) + "T";
       }
       if( ( ( tmp = ( value / GIBI ) ) > 0 ) && ( ( value % GIBI ) == 0 ) ) {
           return Long.toString(tmp) + "G";
       }
       if( ( ( tmp = ( value / MEBI ) ) > 0 ) && ( ( value % MEBI ) == 0 ) ) {
           return Long.toString(tmp) + "M";
       }
       if( ( ( tmp = ( value / KIBI ) ) > 0 ) && ( ( value % KIBI ) == 0 ) ) {
           return Long.toString(tmp) + "K";
       }
       return Long.toString(value);
    }
    public long longValue(){ return _value ; }
    public static void main( String [] args ){
        for (String arg : args) {
            UnitInteger ui = new UnitInteger(arg);
            System.out.println(arg + " -> " + ui
                    .toUnitString() + "  (" + ui + ")");
        }
       System.exit(0);
    }
}

