
package diskCacheV111.services.space;

import java.util.*;

public final class TextParser {
	private static final String WHITESPACE_AND_QUOTES = " \t\r\n\"";
	private static final String DOUBLE_QUOTE          = "\"";
	private String _text;
	private Collection set;

	public TextParser( String text ) {
		if ( text == null ) {
			throw new IllegalArgumentException("Search Text cannot be null.");
		}
		_text = text;
		parseText();
	}

	public String[] getTokens() {
		String result[] = new String[set.size()];
		Iterator i=set.iterator();
		int j=0;
		while(i.hasNext()) {
			result[j]=(String)i.next();
			j++;
		}
		return result;
	}

	public void parseText() {
		set              = new LinkedList();
		boolean returnTokens     = true;
		String currentDelimeters = WHITESPACE_AND_QUOTES;
		StringTokenizer parser   = new StringTokenizer(_text,
							       currentDelimeters,
							       returnTokens);
		String token;
		while(parser.hasMoreTokens()) {
			token = parser.nextToken(currentDelimeters);
			if (!isDoubleQuote(token)){
				addToken(token,set);
			}
			else {
				currentDelimeters = flipDelimiters(currentDelimeters);
			}
		}
	}

	public Collection getTokenSet() {
		return set;
	}


	private boolean emptyText(String text) {
		return (text == null) || (text.trim().equals(""));
	}

	private void addToken(String token, Collection result ){
		if (!emptyText(token)) {
			result.add(token.trim());
		}
	}

	private boolean isDoubleQuote(String token){
		return token.equals(DOUBLE_QUOTE);
	}

	private String flipDelimiters(String currentDelimeters ) {
		String result;
		if ( currentDelimeters.equals(WHITESPACE_AND_QUOTES) ) {
			result = DOUBLE_QUOTE;
		}
		else {
			result = WHITESPACE_AND_QUOTES;
		}
		return result;
	}
}
