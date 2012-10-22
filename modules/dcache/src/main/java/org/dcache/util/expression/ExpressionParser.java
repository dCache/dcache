package org.dcache.util.expression;

import org.parboiled.BaseParser;
import org.parboiled.Rule;
import org.parboiled.Action;
import org.parboiled.Context;
import org.parboiled.support.Var;
import org.parboiled.annotations.*;
import edu.umd.cs.findbugs.annotations.SuppressWarnings;

@BuildParseTree
public abstract class ExpressionParser extends BaseParser<Expression>
{
    public Rule Top() {
        return Sequence(If(), EOI);
    }

    @SuppressWarnings(value="IL_INFINITE_RECURSIVE_LOOP") // NB. Parboil injects code to prevent infinite loops
    Rule If() {
        return Sequence(Disjunction(),
                        Optional(QUERY, If(), COLON, If(),
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
                                  FirstOf(EQ, NE, LE, LT, GE, GT));
    }

    Rule Additive() {
        return BinaryOperatorRule(Multiplicative(), FirstOf(PLUS, MINUS));
    }

    Rule Multiplicative() {
        return BinaryOperatorRule(Match(), FirstOf(MULT, DIV, MOD));
    }

    Rule Match() {
        return BinaryOperatorRule(Unary(), FirstOf(MATCH, NOT_MATCH));
    }

    Rule Unary() {
        Var<Boolean> neg = new Var<>();
        return Sequence(neg.set(false),
                        ZeroOrMore(FirstOf(PLUS,
                                           Sequence(MINUS, neg.set(!neg.get()))
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
        return FirstOf(Sequence(LPAR, If(), RPAR),
                       Literal(),
                       QualifiedIdentifier());
    }

    Rule Literal() {
        return Sequence(FirstOf(Sequence("true", TestNot(LetterOrDigit()),
                                         push(new Expression(Token.TRUE))),
                                Sequence("false", TestNot(LetterOrDigit()),
                                         push(new Expression(Token.FALSE))),
                                Number(),
                                StringLiteral()),
                        Spacing());

    }

    Rule StringLiteral() {
        return FirstOf(Sequence('"',
                                ZeroOrMore(Sequence(TestNot(AnyOf("\r\n\"\\")), ANY)
                                           ).suppressSubnodes(),
                                push(new Expression(Token.STRING_LITERAL,
                                                    match())),
                                '"'
                                ),
                       Sequence('\'',
                                ZeroOrMore(Sequence(TestNot(AnyOf("\r\n'\\")), ANY)
                                           ).suppressSubnodes(),
                                push(new Expression(Token.STRING_LITERAL,
                                                    match())),
                                '\''
                                )
                       );
    }

    Rule QualifiedIdentifier() {
        return Sequence(Sequence(Identifier(),
                                 ZeroOrMore(Ch('.'), Identifier())),
                        push(new Expression(Token.IDENTIFIER, match())),
                        Spacing());
    }

    Rule Identifier() {
        return Sequence(Letter(), ZeroOrMore(LetterOrDigit()));
    }

    Rule Letter() {
        return FirstOf(CharRange('a', 'z'), CharRange('A', 'Z'), '_', '$');
    }

    Rule LetterOrDigit() {
        return FirstOf(CharRange('a', 'z'), CharRange('A', 'Z'), CharRange('0', '9'), '_', '$');
    }

    @SuppressWarnings(value="IL_INFINITE_RECURSIVE_LOOP") // NB. Parboil injects code to prevent infinite loops
    @Cached
    Rule UnaryOperatorRule(Rule subRule, Rule operatorRule) {
        Var<Token> op = new Var<>();
        return FirstOf(Sequence(operatorRule,
                                op.set(Token.find(match().trim())),
                                UnaryOperatorRule(subRule, operatorRule),
                                push(new Expression(op.get(), pop()))),
                       subRule);
    }

    Rule BinaryOperatorRule(Rule subRule, Rule operatorRule) {
        Var<Token> op = new Var<>();
        return Sequence(subRule,
                        ZeroOrMore(operatorRule,
                                   op.set(Token.find(match().trim())),
                                   subRule,
                                   push(new Expression(op.get(), pop(1), pop()))
                                   )
                        );
    }

    Rule Number() {
        return Sequence(Sequence(OneOrMore(Digit()),
                                 Optional(Ch('.'), OneOrMore(Digit()))
                                 ),
                        push(new Expression(Double.parseDouble(match()))),
                        Spacing(),
                        Optional(Unit(),
                                 push(new Expression(Token.MULT,
                                                     pop(), pop()))
                                 )
                        );
    }

    Rule Unit() {
        return FirstOf(UnitRule("Ki", 1L << 10),
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
        return Sequence(s, push(new Expression(factor)));
    }

    Rule Digit() {
        return CharRange('0', '9');
    }

    @SuppressNode
    Rule Spacing() {
        return ZeroOrMore(AnyOf(" \t\r\n\f"));
    }

    @SuppressNode
    @DontLabel
    Rule Terminal(String string) {
        return Sequence(string, Spacing()).label(string);
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