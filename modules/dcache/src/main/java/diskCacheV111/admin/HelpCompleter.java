package diskCacheV111.admin;

import com.google.common.base.CharMatcher;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import jline.console.completer.Completer;

/**
 * Simple completer for JLine that uses the dCache help output to suggest command completions.
 */
public class HelpCompleter implements Completer {

    final List<String> completions = new ArrayList<>();

    public HelpCompleter(String help) {
        for (String line : help.split("\n")) {
            completions.add(scan(line.trim()));
        }
        Collections.sort(completions);
    }

    protected String scan(String line) {
        int i = CharMatcher.anyOf("#[]<>|").indexIn(line);
        int j = line.indexOf(" -");
        i = i == -1 || j == -1 ? Math.max(i, j) : Math.min(i, j);
        return (i == -1) ? line : line.substring(0, i).trim();
    }

    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates) {
        if (cursor < buffer.length()) {
            return -1;
        }
        String s = buffer.replaceAll("\\s+", " ");
        int i = Collections.binarySearch(completions, s);
        if (i < 0) {
            i = -i - 1;
        }
        while (i < completions.size() && completions.get(i).startsWith(s)) {
            candidates.add(completions.get(i));
            i++;
        }
        return 0;
    }
}
