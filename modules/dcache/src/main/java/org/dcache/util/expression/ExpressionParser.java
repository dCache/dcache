package org.dcache.util.expression;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.parboiled.BaseParser;
import org.parboiled.Rule;
import org.parboiled.annotations.BuildParseTree;
import org.parboiled.annotations.Cached;
import org.parboiled.annotations.DontLabel;
import org.parboiled.annotations.SuppressNode;
import org.parboiled.support.Var;

@BuildParseTree
public abstract class ExpressionParser extends BaseParser<Expression>
{
    public Rule Top() {
        return sequence(If(), EOI);
    }

    @SuppressWarnings("InfiniteRecursion")
    @SuppressFBWarnings(value="IL_INFINITE_RECURSIVE_LOOP",
            justification = "Parboil injects code to prevent infinite loops")
    Rule If() {
        return sequence(Disjunction(),
                        optional(QUERY, If(), COLON, If(),
                                 push(new Expression(Token.IF,
                                                     pop(2), pop(1), pop()))));
    }

    Rule Disjunction() {
        return BinaryOperatorRule(Conjunction(), OR);
    }

    Rule Conjunction() {
        return BinaryOperatorRule(Negation(), AND);
    }

    Rule Negation() {
        return UnaryOperatorRule(Relational(), NOT);
    }

    Rule Relational() {
        return BinaryOperatorRule(Additive(),
                                  firstOf(EQ, NE, LE, LT, GE, GT));
    }

    Rule Additive() {
        return BinaryOperatorRule(Multiplicative(), firstOf(PLUS, MINUS));
    }

    Rule Multiplicative() {
        return BinaryOperatorRule(Match(), firstOf(MULT, DIV, MOD));
    }

    Rule Match() {
        return BinaryOperatorRule(Unary(), firstOf(MATCH, NOT_MATCH));
    }

    Rule Unary() {
        Var<Boolean> neg = new Var<>();
        return sequence(neg.set(false),
                        zeroOrMore(firstOf(PLUS,
                                           sequence(MINUS, neg.set(!neg.get()))
                                           )
                                   ),
                        Power(),
                        neg.get()
                        ? push(new Expression(Token.UMINUS, pop()))
                        : true);
    }

    Rule Power() {
        return BinaryOperatorRule(Primary(), POWER);
    }

    Rule Primary() {
        return firstOf(sequence(LPAR, If(), RPAR),
                       Literal(),
                       QualifiedIdentifier());
    }

    Rule Literal() {
        return sequence(firstOf(sequence("true", testNot(LetterOrDigit()),
                                         push(new Expression(Token.TRUE))),
                                sequence("false", testNot(LetterOrDigit()),
                                         push(new Expression(Token.FALSE))),
                                Number(),
                                StringLiteral()),
                        Spacing());

    }

    Rule StringLiteral() {
        return firstOf(sequence('"',
                                zeroOrMore(sequence(testNot(anyOf("\r\n\"\\")), ANY)
                                           ).suppressSubnodes(),
                                push(new Expression(Token.STRING_LITERAL,
                                                    match())),
                                '"'
                                ),
                       sequence('\'',
                                zeroOrMore(sequence(testNot(anyOf("\r\n'\\")), ANY)
                                           ).suppressSubnodes(),
                                push(new Expression(Token.STRING_LITERAL,
                                                    match())),
                                '\''
                                )
                       );
    }

    Rule QualifiedIdentifier() {
        return sequence(sequence(Identifier(),
                                 zeroOrMore(ch('.'), Identifier())),
                        push(new Expression(Token.IDENTIFIER, match())),
                        Spacing());
    }

    Rule Identifier() {
        return sequence(Letter(), zeroOrMore(LetterOrDigit()));
    }

    Rule Letter() {
        return firstOf(charRange('a', 'z'), charRange('A', 'Z'), '_', '$');
    }

    Rule LetterOrDigit() {
        return firstOf(charRange('a', 'z'), charRange('A', 'Z'), charRange('0', '9'), '_', '$');
    }

    @SuppressWarnings("InfiniteRecursion")
    @SuppressFBWarnings(value = "IL_INFINITE_RECURSIVE_LOOP",
            justification = "Parboil injects code to prevent infinite loops")
    @Cached
    Rule UnaryOperatorRule(Rule subRule, Rule operatorRule) {
        Var<Token> op = new Var<>();
        return firstOf(sequence(operatorRule,
                                op.set(Token.find(match().trim())),
                                UnaryOperatorRule(subRule, operatorRule),
                                push(new Expression(op.get(), pop()))),
                       subRule);
    }

    Rule BinaryOperatorRule(Rule subRule, Rule operatorRule) {
        Var<Token> op = new Var<>();
        return sequence(subRule,
                        zeroOrMore(operatorRule,
                                   op.set(Token.find(match().trim())),
                                   subRule,
                                   push(new Expression(op.get(), pop(1), pop()))
                                   )
                        );
    }

    Rule Number() {
        return sequence(sequence(oneOrMore(Digit()),
                                 optional(ch('.'), oneOrMore(Digit()))
                                 ),
                        push(new Expression(Double.parseDouble(match()))),
                        Spacing(),
                        optional(Unit(),
                                 push(new Expression(Token.MULT,
                                                     pop(), pop()))
                                 )
                        );
    }

    Rule Unit() {
        return firstOf(UnitRule("Ki", 1L << 10),
                       UnitRule("Mi", 1L << 20),
                       UnitRule("Gi", 1L << 30),
                       UnitRule("Ti", 1L << 40),
                       UnitRule("Pi", 1L << 50),
                       UnitRule("k", 1000L),
                       UnitRule("K", 1000L),
                       UnitRule("M", 1000L * 1000),
                       UnitRule("G", 1000L * 1000 * 1000),
                       UnitRule("T", 1000L * 1000 * 1000 * 1000),
                       UnitRule("P", 1000L * 1000 * 1000 * 1000 * 1000)
                       );
    }

    Rule UnitRule(String s, long factor) {
        return sequence(s, push(new Expression(factor)));
    }

    Rule Digit() {
        return charRange('0', '9');
    }

    @SuppressNode
    Rule Spacing() {
        return zeroOrMore(anyOf(" \t\r\n\f"));
    }

    @SuppressNode
    @DontLabel
    Rule Terminal(String string) {
        return sequence(string, Spacing()).label(string);
    }

    final Rule AND = Terminal(Token.AND.label);
    final Rule COLON = Terminal(":");
    final Rule DIV = Terminal(Token.DIV.label);
    final Rule EQ = Terminal(Token.EQ.label);
    final Rule GE = Terminal(Token.GE.label);
    final Rule GT = Terminal(Token.GT.label);
    final Rule LE = Terminal(Token.LE.label);
    final Rule LPAR = Terminal("(");
    final Rule LT = Terminal(Token.LT.label);
    final Rule MATCH = Terminal(Token.MATCH.label);
    final Rule MINUS = Terminal(Token.MINUS.label);
    final Rule MOD = Terminal(Token.MOD.label);
    final Rule MULT = Terminal(Token.MULT.label);
    final Rule NE = Terminal(Token.NE.label);
    final Rule NOT = Terminal(Token.NOT.label);
    final Rule NOT_MATCH = Terminal(Token.NOT_MATCH.label);
    final Rule OR = Terminal(Token.OR.label);
    final Rule PLUS = Terminal(Token.PLUS.label);
    final Rule POWER = Terminal(Token.POWER.label);
    final Rule QUERY = Terminal("?");
    final Rule RPAR = Terminal(")");
}
