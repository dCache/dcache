package diskCacheV111.admin ;

import com.google.common.base.CharMatcher;
import jline.Completor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Simple completor for JLine that uses the dCache help output to
 * suggest command completions.
 */
public class HelpCompletor implements Completor
{
    final List<String> completions = new ArrayList<>();

    public HelpCompletor(String help)
    {
        for (String line: help.split("\n")) {
            completions.add(scan(line.trim()));
        }
        Collections.sort(completions);
    }

    protected String scan(String line)
    {
        int i = CharMatcher.anyOf("#[]<>|-").indexIn(line);
        return (i == -1) ? line : line.substring(0, i);
    }

    @Override
    public int complete(String buffer, int cursor, List candidates)
    {
        if (cursor < buffer.length()) {
            return -1;
        }
        String s = buffer.replaceAll("\\s+", " ");
        int i = Collections.binarySearch(completions, s);
        if (i < 0) {
            i =  -i - 1;
        }
        while (i < completions.size() && completions.get(i).startsWith(s)) {
            candidates.add(completions.get(i));
            i++;
        }
        return 0;
    }
}
