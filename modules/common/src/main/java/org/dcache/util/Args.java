package org.dcache.util;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

import java.io.Serializable;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static java.util.Arrays.asList;

/**
 * Argument parser.
 *
 * Supports single and multi character options, although both start
 * with a single hyphen. Options may have an optional value using the
 * equal sign to separate key and value.
 *
 * Supports single and double quoted valus and uses backslash as an
 * escape symbol.
 *
 * An optional double hyphen argument separator separates options from
 * arguments. Any argument following the separator will not be
 * interpreted as an option.
 *
 * Option and argument order is preserved with some limitations:
 * Option and argument interleaving is not preserved. Repeated options
 * are preserved, including the order of their values, however all
 * values appear at the place of the first use of that option.
 */
public class Args implements Serializable
{
    private static final long serialVersionUID = 4389995682226525641L;
    private final ImmutableListMultimap<String,String> _options;
    private final String _oneChar;
    private ImmutableList<String> _arguments;

    public Args(CharSequence args)
    {
        Scanner scanner = new Scanner();
        scanner.scan(args, false);
        _options = scanner.options.build();
        _arguments = scanner.arguments.build();
        _oneChar = scanner.oneChar.toString();
    }

    public Args(String[] args)
    {
        Scanner scanner = new Scanner();
        for (String arg : args) {
            if (arg.isEmpty()) {
                scanner.arguments.add("");
            } else {
                scanner.scan(arg, true);
            }
        }
        _options = scanner.options.build();
        _arguments = scanner.arguments.build();
        _oneChar = scanner.oneChar.toString();
    }

    public Args(Args in)
    {
        _arguments = in._arguments;
        _options = in._options;
        _oneChar = in._oneChar;
    }

    private Args(ImmutableList<String> arguments,
            ImmutableListMultimap<String,String> options, String oneChar)
    {
        _arguments = arguments;
        _options = options;
        _oneChar = oneChar;
    }

    public static CharSequence quote(String raw)
    {
        StringBuilder sb = new StringBuilder();
        quote(raw, sb);
        return sb;
    }

    public Args removeOptions(String... names)
    {
        ListMultimap<String,String> view =
                Multimaps.filterKeys(_options, not(in(asList(names))));

        return new Args(_arguments, ImmutableListMultimap.copyOf(view), _oneChar);
    }

    public boolean isOneCharOption(char c)
    {
        return _oneChar.indexOf(c) > -1;
    }

    public int argc()
    {
        return _arguments.size();
    }

    public int optc()
    {
        return _options.size();
    }

    public String getOpt(String name)
    {
        return getOption(name);
    }

    public double getDoubleOption(String name)
    {
        String option = getOption(name);

        if (option == null) {
            throw new NoSuchElementException("Argument "
                                             + name + " does not exist.");
        }

        return Double.parseDouble(option);
    }

    public double getDoubleOption(String name, double defaultValue)
    {
        String option = getOption(name);

        if (option == null) {
            return defaultValue;
        } else if (option.isEmpty()) {
            throw new IllegalArgumentException("Argument " + name +
                                               " does not have a value.");
        } else {
            return Double.parseDouble(option);
        }
    }

    public int getIntOption(String name)
    {
        String option = getOption(name);

        if (option == null) {
            throw new NoSuchElementException("Argument "
                                             + name + " does not exist.");
        }

        return Integer.parseInt(option);
    }

    public int getIntOption(String name, int defaultValue)
    {
        String option = getOption(name);

        if (option == null) {
            return defaultValue;
        } else if (option.isEmpty()) {
            throw new IllegalArgumentException("Argument " + name +
                                               " does not have a value.");
        } else {
            return Integer.parseInt(option);
        }
    }

    public long getLongOption(String name)
    {
        String option = getOption(name);

        if (option == null) {
            throw new NoSuchElementException("Argument "
                                             + name + " does not exist.");
        }

        return Long.parseLong(option);
    }

    public long getLongOption(String name, long defaultValue)
    {
        String option = getOption(name);

        if (option == null) {
            return defaultValue;
        } else if (option.isEmpty()) {
            throw new IllegalArgumentException("Argument " + name +
                                               " does not have a value.");
        } else {
            return Long.parseLong(option);
        }
    }

    public String getOption(String name)
    {
        ImmutableList<String> values = _options.get(name);
        return values.isEmpty() ? null : values.get(values.size() - 1);
    }

    public String getOption(String name, String defaultValue) {
        ImmutableList<String> values = _options.get(name);
        return values.isEmpty() ? defaultValue : values.get(values.size() - 1);
    }

    public boolean hasOption(String name)
    {
        return !_options.get(name).isEmpty();
    }

    public ImmutableList<String> getOptions(String name)
    {
        return _options.get(name);
    }

    public String argv(int i)
    {
        return (i < _arguments.size()) ? _arguments.get(i) : null;
    }

    public ImmutableList<String> getArguments()
    {
        return _arguments;
    }

    public String optv(int i)
    {
        ImmutableMultiset<String> keys = _options.keys();
        return (i < keys.size()) ? keys.asList().get(i) : null;
    }

    public void shift()
    {
        if (!_arguments.isEmpty()) {
            _arguments = _arguments.subList(1, _arguments.size());
        }
    }

    public void shift(int n)
    {
        while (n-- > 0) {
            shift();
        }
    }

    public ImmutableListMultimap<String,String> options()
    {
        return _options;
    }

    public ImmutableMap<String,String> optionsAsMap()
    {
        ImmutableMap.Builder<String,String> builder = ImmutableMap.builder();
        for (Map.Entry<String,String> e: _options.entries()) {
            builder.put(e.getKey(), e.getValue());
        }
        return builder.build();
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }
        if (!(other instanceof Args)) {
            return false;
        }

        Args args = (Args) other;
        return args._options.equals(_options)
            && args._arguments.equals(_arguments)
            && args._oneChar.equals(_oneChar);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(_options, _arguments);
    }

    private static void quote(String in, StringBuilder out)
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

    @Override
    public String toString()
    {
        StringBuilder s = new StringBuilder();

        for (Map.Entry<String,String> e: _options.entries()) {
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

        Joiner.on(' ').appendTo(s, _arguments);

        return s.toString();
    }

    public String getInfo()
    {
        StringBuilder sb = new StringBuilder();

        sb.append("Positional :\n");
        for (int i = 0; i < _arguments.size(); i++) {
            sb.append(i).append(" -> ").append(_arguments.get(i)).append("\n");
        }
        sb.append("Options :\n");

        for (Map.Entry<String,String> option: _options.entries()) {
            sb.append(option.getKey());
            if (option.getValue() != null) {
                sb.append(" -> ").append(option.getValue());
            }
            sb.append("\n");
        }

        return sb.toString();
    }

   public static void main( String [] args )
   {
      if( args.length < 1 ){
         System.err.println( "Usage : ... <parseString>" ) ;
         System.exit(4);
      }
      Args lineArgs;
      if( args.length == 1 ) {
          lineArgs = new Args(args[0]);
      } else {
          lineArgs = new Args(args);
      }
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
    private static class Scanner
    {
        final ImmutableListMultimap.Builder<String,String> options = new ImmutableListMultimap.Builder<>();
        final ImmutableList.Builder<String> arguments = new ImmutableList.Builder<>();
        final StringBuilder oneChar = new StringBuilder();

        private CharSequence line;
        private int position;
        private boolean isAtEndOfOptions;
        private boolean shouldIgnoreWhitespace;

        public Scanner()
        {
        }

        private char peek()
        {
            return isEof() ? (char) 0 : line.charAt(position);
        }

        private char readChar()
        {
            char c = peek();
            position++;
            return c;
        }

        private boolean isEof()
        {
            return (position >= line.length());
        }

        private boolean isWhitespace()
        {
            return !shouldIgnoreWhitespace && Character.isWhitespace(peek());
        }

        private void scanWhitespace()
        {
            while (isWhitespace()) {
                readChar();
            }
        }

        public void scan(CharSequence line, boolean shouldIgnoreWhitespace)
        {
            this.line = line;
            this.shouldIgnoreWhitespace = shouldIgnoreWhitespace;
            position = 0;

            scanWhitespace();
            while (!isEof()) {
                if (!isAtEndOfOptions && peek() == '-') {
                    readChar();
                    String key = scanKey();
                    if (key.isEmpty()) {
                        arguments.add("-");
                    } else if (peek() == '=') {
                        readChar();
                        options.put(key, scanWord());
                    } else if (key.equals("-")) {
                        isAtEndOfOptions = true;
                    } else {
                        options.put(key, "");
                        oneChar.append(key);
                    }
                } else {
                    arguments.add(scanWord());
                }
                scanWhitespace();
            }
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
