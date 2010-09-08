package org.dcache.util.expression;

public enum Token
{
    AND("and"),
    DIV("/"),
    DOT("."),
    EQ("=="),
    GE(">="),
    GT(">"),
    LE("<="),
    LT("<"),
    MINUS("-"),
    MOD("%"),
    MULT("*"),
    NE("!="),
    NOT("not"),
    OR("or"),
    PLUS("+"),
    POWER("**"),
    MATCH("=~"),
    NOT_MATCH("!~"),
    IF("?"),
    TRUE("true"),
    FALSE("false"),
    UMINUS(""),
    NUMBER_LITERAL(""),
    STRING_LITERAL(""),
    IDENTIFIER("");

    public final String label;

    Token(String label) {
        this.label = label;
    }

    public static Token find(String label)
    {
        for (Token token: values()) {
            if (token.label.equals(label)) {
                return token;
            }
        }
        throw new IllegalArgumentException(label);
    }
}
