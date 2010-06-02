package dmg.util ;

import java.util.* ;
import java.io.Serializable;

/**
 * TODO: Write JavaDoc!
 */
public class Args
    implements Serializable, Cloneable
{

   static final long serialVersionUID = -8950082352156787965L;
   private final Map<String, String> _optHash = CollectionFactory.newHashMap();
   private final List<String>    _optv  = new Vector<String>();
   private final List<String>    _argv  = new Vector<String>();
   private String _oneChar;
   public Args( String args ) {

       new Scanner(args).scan();
   }
   public Args( String [] args ) {

      StringBuilder sb = new StringBuilder() ;
      for( int i = 0 ; i < args.length ; i++ )
         sb.append(args[i]).append(" ");

      new Scanner(sb).scan();
   }
   Args( Args in ){
     _argv.addAll(in._argv);
     _optv.addAll( in._optv );
     _optHash.putAll( in._optHash );
     _oneChar = in._oneChar;
   }
   public boolean isOneCharOption( char c ){
     return _oneChar.indexOf(c) > -1 ;
   }
   public int argc(){ return _argv.size() ; }
   public int optc(){ return _optv.size() ; }
   public String getOpt( String optName ){ return _optHash.get( optName ) ; }
   public String argv( int i ){

	   String value = null;
       if( i < _argv.size() ) {
         value =  _argv.get(i) ;
       }

       return value;

   }
   public String optv( int i ){
	   String value = null;

      if( i < _optv.size() ){
        value =  _optv.get(i) ;
      }

      return value;
   }
   public void shift(){

      if( !_argv.isEmpty() ) {
        _argv.remove(0);
      }

   }

    public Map<String, String>  options()
    {
        return Collections.unmodifiableMap(_optHash);
    }

   public Object clone(){ return new Args( this ) ; }

    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }
        if (!(other instanceof Args)) {
            return false;
        }

        Args args = (Args) other;
        return args._optHash.equals(_optHash)
            && args._optv.equals(_optv)
            && args._argv.equals(_argv)
            && args._oneChar.equals(_oneChar);
    }

    public int hashCode()
    {
        return _optHash.hashCode() ^ _argv.hashCode();
    }

    private void quote(String in, StringBuilder out)
    {
        for (int i = 0; i < in.length(); i++) {
            switch (in.charAt(i)) {
            case '\\':
                out.append("\\\\");
                break;
            case '"':
                out.append("\\\"");
                break;
            case '\'':
                out.append("\\'");
                break;
            case '=':
                out.append("\\=");
                break;
            case ' ':
                out.append("\\ ");
                break;
            default:
                out.append(in.charAt(i));
                break;
            }
        }
    }

    public String toString()
    {
        StringBuilder s = new StringBuilder();

        for (Map.Entry<String,String> e: _optHash.entrySet()) {
            String key = e.getKey();
            String value = e.getValue();

            s.append('-');
            quote(key, s);
            if (value.length() > 0) {
                s.append('=');
                quote(value, s);
            }
            s.append(' ');
        }

        if (s.length() > 0) {
            s.append("-- ");
        }

        for (int i = 0; i < argc(); i++) {
            quote(argv(i), s);
            s.append(' ');
        }

        return s.toString();
    }

   public String getInfo(){
      StringBuilder sb = new StringBuilder() ;

      sb.append( "Positional :\n" );
      for( int i= 0 ; i < _argv.size() ; i++ ){
         sb.append(i).append(" -> ").append(_argv.get(i)).append("\n") ;
      }
      sb.append( "Options :\n" );
      for( int i= 0 ; i < _optv.size() ; i++ ){
         String key = _optv.get(i) ;
         String val = _optHash.get(key) ;
         sb.append(key) ;
         if( val != null )
            sb.append( " -> " ).append(val) ;
         sb.append("\n") ;
      }

      return sb.toString() ;
   }

   public static void main( String [] args )throws Exception {
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

    /**
     * Scanner for parsing strings of white space separated
     * words. Characters may be escaped with a backslash and character
     * sequences may be quoted. Options begin with an unescaped dash.

     * A -- signals the end of options and disables further option
     * processing.  Any arguments after the -- are treated as regular
     * arguments.
     */
    class Scanner
    {
        private final CharSequence _line;
        private int _position;

        public Scanner(CharSequence line)
        {
            _line = line;
        }

        private char peek()
        {
            return isEof() ? (char) 0 : _line.charAt(_position);
        }

        private char readChar()
        {
            char c = peek();
            _position++;
            return c;
        }

        private boolean isEof()
        {
            return (_position >= _line.length());
        }

        private boolean isWhitespace()
        {
            return Character.isWhitespace(peek());
        }

        private void scanWhitespace()
        {
            while (isWhitespace()) {
                readChar();
            }
        }

        public void scan()
        {
            StringBuilder oneChar = new StringBuilder();
            boolean isAtEndOfOptions = false;
            scanWhitespace();
            while (!isEof()) {
                if (!isAtEndOfOptions && peek() == '-') {
                    readChar();
                    String key = scanKey();
                    if (key.isEmpty()) {
                        _argv.add("-");
                    } else if (peek() == '=') {
                        readChar();
                        _optv.add(key) ;
                        _optHash.put(key, scanWord());
                    } else if (key.equals("-")) {
                        isAtEndOfOptions = true;
                    } else {
                        _optv.add(key) ;
                        _optHash.put(key, "");
                        oneChar.append(key);
                    }
                } else {
                    _argv.add(scanWord());
                }
                scanWhitespace();
            }
            _oneChar = oneChar.toString();
        }

        /**
         * Scans an option key. An option key is terminated by an
         * unescaped white space character or - for non-empty keys -
         * by an unescaped equal sign.
         */
        private String scanKey()
        {
            StringBuilder key = new StringBuilder();
            do {
                scanWordElement(key);
            } while (!isEof() && !isWhitespace() && peek() != '=');
            return key.toString();
        }

        /**
         * Scans the next word. A word is a sequence of non-white
         * space characters and escaped or quoted white space
         * characters. The unescaped and unquoted word is returned.
         */
        private String scanWord()
        {
            StringBuilder word = new StringBuilder();
            while (!isEof() && !isWhitespace()) {
                scanWordElement(word);
            }
            return word.toString();
        }

        /**
         * Scans the next element of a word. Elements of a word are
         * non-white space characters, escaped characters and quoted
         * strings. The unescaped and unquoted element is added to word.
         */
        private void scanWordElement(StringBuilder word)
        {
            if (!isEof() && !isWhitespace()) {
                switch (peek()) {
                case '\'':
                    scanSingleQuotedString(word);
                    break;
                case '"':
                    scanDoubleQuotedString(word);
                    break;
                case '\\':
                    scanEscapedCharacter(word);
                    break;
                default:
                    word.append(readChar());
                    break;
                }
            }
        }

        /**
         * Scans a single quoted string. Escaped characters are not
         * recognized. The unquoted string is added to word.
         */
        private void scanSingleQuotedString(StringBuilder word)
        {
            if (readChar() != '\'') {
                throw new IllegalStateException("Parse failure");
            }

            while (!isEof()) {
                char c = readChar();
                switch (c) {
                case '\'':
                    return;
                default:
                    word.append(c);
                    break;
                }
            }
        }

        /**
         * Scans a double quoted string. Escaped characters are
         * recognized. The unquoted and unescaped string is added to
         * word.
         */
        private void scanDoubleQuotedString(StringBuilder word)
        {
            if (readChar() != '"') {
                throw new IllegalStateException("Parse failure");
            }

            while (!isEof()) {
                switch (peek()) {
                case '\\':
                    scanEscapedCharacter(word);
                    break;
                case '"':
                    readChar();
                    return;
                default:
                    word.append(readChar());
                    break;
                }
            }
        }

        /**
         * Scans a backslash escaped character. The escaped character
         * without the escape symbol is added to word.
         */
        private void scanEscapedCharacter(StringBuilder word)
        {
            if (readChar() != '\\') {
                throw new IllegalStateException("Parse failure");
            }

            if (!isEof()) {
                word.append(readChar());
            }
        }
    }
}
