package dmg.util;

import java.util.ArrayDeque;
import java.util.Deque;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class Formats {

  public static final int CENTER   =  0x1 ;
  public static final int RIGHT    =  0x4 ;
  public static final int LEFT     =  0x2 ;
  public static final int CUT      =  0x8 ;

  public static String field( String in , int field ){
     return field(in,field,LEFT) ;
  }
  public static String field( String in , int field , int flags ){
    if( in.length() >= field ){
      if( ( flags & CUT ) > 0 ){
         return in.substring( 0 , field+1 ) ;
      }else{
         return in ;
      }
    }
    StringBuilder sb = new StringBuilder() ;
    int i ;
    if( ( flags & CENTER ) > 0 ){
       int diff  = field - in.length() ;
       int left  = diff / 2 ;
       int right = diff - left ;
       for( i = 0 ; i < left ; i++ ) {
           sb.append(' ');
       }
       sb.append( in ) ;
       for( i = 0 ; i < right ; i++ ) {
           sb.append(' ');
       }
    }else if( ( flags & RIGHT ) > 0 ){
       int diff = field - in.length() ;
       for( i = 0 ; i < diff ; i++ ) {
           sb.append(' ');
       }
       sb.append( in ) ;
    }else{
       sb.append( in ) ;
       int diff = field - in.length() ;
       for( i = 0 ; i < diff ; i++ ) {
           sb.append(' ');
       }
    }
    return sb.toString() ;
  }
  public static String cutClass( String c ){
     int lastDot = c.lastIndexOf( '.' ) ;
     if( ( lastDot < 0 ) || ( lastDot >= ( c.length() - 1 ) ) ) {
         return c;
     }
     return c.substring( lastDot+1 ) ;

  }
  private static final int RP_IDLE   = 0 ;
  private static final int RP_DOLLAR = 1 ;
  private static final int RP_OPENED = 2 ;

    /**
     * Starting after the opening sequence of a placeholder ('$' followed by '{'), this method
     * scans until the end of the placeholder ('}') and pushes a replacement sourced from
     * {@code replaceable} to {@code out}.
     *
     * Handles both recursive, nested and incomplete placeholders.
     *
     * @param in Input sequence to scan
     * @param pos Position of the first character of the key
     * @param out Output sequence
     * @param replaceable Source of replacements for placeholders
     * @param stack Placeholders already replaced in the input sequence - prevents infinite recursion
     * @return Position of the first character after the end of the placeholder
     */
    private static int replaceKey(char[] in, int pos, StringBuilder out, Replaceable replaceable, Deque<String> stack)
    {
        int mark = pos;
        int length = in.length;
        StringBuilder key = new StringBuilder(length);
        char c1;
        while (pos < length && (c1 = in[pos]) != '}') {
            if (c1 != '$' || pos + 1 == length) {
                /* Regular character */
                pos++;
            } else {
                key.append(in, mark, pos - mark);
                char c2 = in[pos + 1];
                switch (c2) {
                case '{':
                    /* Nested placeholder */
                    pos = replaceKey(in, pos + 2, key, replaceable, stack);
                    break;
                case '$':
                    /* Escaped $ symbol */
                    key.append('$');
                    pos += 2;
                    break;
                default:
                    /* False alarm - sequence got no special meaning */
                    key.append(c1).append(c2);
                    pos += 2;
                    break;
                }
                mark = pos;
            }
        }
        key.append(in, mark, pos - mark);

        if (pos < length) {
            /* Complete placeholder */
            pos++;

            /* Lookup replacement value */
            String keyName = key.toString();
            String keyValue = replaceable.getReplacement(keyName);

            /* Unless we already replaced in once in the current stack, replace any placeholders in the value */
            if (keyValue == null || stack.contains(keyName)) {
                out.append("${").append(keyName).append('}');
            } else {
                stack.push(keyName);
                replaceKeywords(keyValue.toCharArray(), out, replaceable, stack);
                stack.pop();
            }
        } else {
            /* Incomplete placeholder - got to end of input sequence without closing curly brace */
            out.append("${").append(key);
        }

        return pos;
    }

    /**
     * Replaces placeholders in {@code in}, writing the result to {@code out}. Replacements for
     * placeholders are sourced from {@code replaceable}.
     *
     * @param in Input sequence
     * @param out Output sequence
     * @param replaceable Source of replacements for placeholders
     * @param stack Placeholders already replaced in the input sequence - prevents infinite recursion
     * @return {@code out}
     */
    private static StringBuilder replaceKeywords(char[] in, StringBuilder out, Replaceable replaceable, Deque<String> stack)
    {
        int pos = 0;
        int length = in.length;
        int mark = 0;
        while (pos < length) {
            char c1 = in[pos];
            if (c1 != '$' || pos + 1 == length) {
                /* Regular character */
                pos++;
            } else {
                out.append(in, mark, pos - mark);
                char c2 = in[pos + 1];
                switch (c2) {
                case '{':
                    /* Placeholder */
                    pos = replaceKey(in, pos + 2, out, replaceable, stack);
                    break;
                case '$':
                    /* Escaped $ symbol */
                    out.append('$');
                    pos += 2;
                    break;
                default:
                    /* False alarm - sequence got no special meaning */
                    out.append(c1).append(c2);
                    pos += 2;
                    break;
                }
                mark = pos;
            }
        }
        out.append(in, mark, pos - mark);
        return out;
    }

    public static String replaceKeywords(String in, Replaceable cb)
    {
        return replaceKeywords(in.toCharArray(), new StringBuilder(in.length()), cb, new ArrayDeque<>()).toString();
    }

/**
 * a useful tool which can interpret jokers (*) and wildcards (?) to filter
 * from a given Array of Strings the matching ones.
 * Written by Manfred Maschewski, DESY
 *
 * @author              Manfred Maschewski, DESY Hamburg
 * @version             0.1             14 Jan 99
 */
  public static boolean match( String condition , String subject ) {
    // handling the joker
    if ( condition.startsWith("*") ) {
      // removing the joker
      condition = condition.substring( 1 ) ;
      int nextJoker = condition.indexOf('*') ;
      if ( nextJoker == -1 ) {
        return endsWith( subject, condition ) ;
      } else {
        String firstPart = condition.substring( 0, nextJoker ) ;
        int position = indexOf( subject, firstPart ) ;
        if ( position == -1 ) {
          return false;
        } else {
          condition = condition.substring( firstPart.length() ) ;
          subject = subject.substring( position + firstPart.length() ) ;
          return match( condition , subject  ) ;
        }
      }
    } else {
      int nextJoker = condition.indexOf('*') ;
      if ( nextJoker == -1 ) {
        return equals( subject, condition ) ;
      } else {
        String firstPart = condition.substring( 0, nextJoker ) ;
        if ( ! startsWith( subject, firstPart ) ) {
                return false ;
        } else {
                condition = condition.substring( firstPart.length() ) ;
                subject = subject.substring( firstPart.length() ) ;
                return match( condition , subject ) ;
        }
      }
    }
  }


  private static boolean startsWith( String subject, String term ) {
     int riddle ;
     subject = subject.substring( 0, term.length() ) ;
     while (( riddle = term.indexOf('?')) != -1 ) {
       term = removeChar(term,riddle) ;
       subject = removeChar(subject,riddle) ;
     }
     return equals( subject, term ) ;
  }

  private static boolean endsWith( String subject, String term ) {
    int riddle ;
    // if term is longer than subject it doesn't matches anyway...
    if ( subject.length() < term.length() ) {
      return false ;
    }
    subject = subject.substring( subject.length() - term.length() ) ;
    while (( riddle = term.indexOf('?')) != -1 ) {
      term = removeChar(term,riddle) ;
      subject = removeChar(subject,riddle) ;
    }
    return equals( subject, term ) ;
  }

  private static int indexOf( String subject, String term ) {
    if ( subject.length() < term.length() ) {
      return -1 ;
    }
    // going throught the subject
    outer: for ( int i = 0 ; i < (subject.length() - term.length() + 1); i++ ){
      for ( int j = 0; j < term.length(); j++ ) {
        if ( ! ((term.charAt(j) == subject.charAt(j+i)))
             && ! (term.charAt(j) == '?') ) {
          continue outer ;
        }
      }
      return i ;
    }
    return -1 ;
  }
  private static String removeChar( String str, int i ) {
    str = str.substring(0,i) + str.substring(i+1,str.length()) ;
    return str ;
  }
  private static boolean equals( String subject, String term ) {
     int riddle ;
     while (( riddle = term.indexOf('?')) != -1 ) {
             term = removeChar(term,riddle) ;
             subject = removeChar(subject,riddle) ;
     }
     return subject.equals(term) ;
  }

}

