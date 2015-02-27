package diskCacheV111.admin ;

import jline.console.completer.Completer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Simple completor for JLine that uses the dCache help output to
 * suggest command completions.
 */
public class HelpCompleter implements Completer
{
    final List<String> completions = new ArrayList<>();

    public HelpCompleter(String help)
    {
        for (String line: help.split("\n")) {
            completions.add(scan(line.trim()));
        }
        Collections.sort(completions);
    }

    protected String scan(String line)
    {
        StringBuilder completion = new StringBuilder();
        for (String s: line.split(" ")) {
            if (s.startsWith("#")) {
                break;
            }
            if (!s.contains("[") && !s.contains("<") && !s.contains("|") && !s.startsWith("-")) {
                completion.append(s).append(" ");
            }
        }
        return completion.toString();
    }

    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates)
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
