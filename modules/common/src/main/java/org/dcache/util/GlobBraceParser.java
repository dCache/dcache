/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.util;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import static com.google.common.collect.Iterables.concat;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Simple parser that expands alternation lists in globs recursively.
 *
 * Recognizes the following LL(1) grammar:
 *
 * S   ::=  E T
 * T   ::=  "," S | ""
 * E   ::=  STR F | F
 * F   ::=  "{" S "}" E | ""
 * STR ::=  [^,{}]+
 *
 *
 * The grammar is implemented as a recursive decent parser that unfolds the
 * alternations on the fly. The semantics are defined by the following pseudo
 * code ([] constructs a list, U is union and x is the cartesian product):
 *
 * expand(S) = expand(E) U expand(T)
 * expand(T) = expand(S) | []
 * expand(E) = [ STR ] x expand(F) | expand(F)
 * expand(F) = expand(S) x expand(E) | [""]
 */
class GlobBraceParser
{
    /**
     * Simple lexicographical analyzer with a look ahead of 1.
     */
    private static class Scanner
    {
        private final StringTokenizer tokenizer;
        private String current;

        public Scanner(String s)
        {
            tokenizer = new StringTokenizer(s, ",{}", true);
            current = tokenizer.hasMoreTokens() ? tokenizer.nextToken() : "";
        }

        public String peek()
        {
            return current;
        }

        public String next()
        {
            String current = this.current;
            this.current = tokenizer.hasMoreTokens() ? tokenizer.nextToken() : "";
            return current;
        }
    }

    private final Scanner scanner;

    GlobBraceParser(String s)
    {
        scanner = new Scanner(s);
    }

    Iterable<String> expandGlob()
    {
        Iterable<String> result = expandE();
        checkEndOfInput();
        return result;
    }

    Iterable<String> expandList()
    {
        Iterable<String> result = expandS();
        checkEndOfInput();
        return result;
    }

    private void checkEndOfInput()
    {
        if (!scanner.peek().isEmpty()) {
            throw new IllegalArgumentException("Unexpected token " + scanner.peek());
        }
    }

    private Iterable<String> expandS()
    {
        return concat(expandE(), expandT());
    }

    private Iterable<String> expandT()
    {
        switch (scanner.peek()) {
        case ",":
            scanner.next();
            return expandS();
        default:
            return emptyList();
        }
    }

    private Iterable<String> expandE()
    {
        switch (scanner.peek()) {
        case "{":
        case "}":
        case ",":
            return expandF();
        default:
            return cartesianProduct(singletonList(scanner.next()), expandF());
        }
    }

    private Iterable<String> expandF()
    {
        if (scanner.peek().equals("{")) {
            scanner.next();
            Iterable<String> left = expandS();
            String token = scanner.next();
            if (!token.equals("}")) {
                throw new IllegalArgumentException("Expected '}' instead of '" + token + '\'');
            }
            Iterable<String> right = expandE();
            return cartesianProduct(left, right);
        } else {
            return singletonList("");
        }
    }

    private Iterable<String> cartesianProduct(Iterable<String> left, Iterable<String> right)
    {
        List<String> result = new ArrayList<>();
        for (String s1 : left) {
            for (String s2 : right) {
                result.add(s1 + s2);
            }
        }
        return result;
    }
}
