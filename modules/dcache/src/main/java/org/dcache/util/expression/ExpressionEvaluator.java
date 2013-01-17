package org.dcache.util.expression;

import static org.dcache.util.expression.Type.*;
import static org.dcache.util.expression.Token.*;

/**
 * Interpreter for the expression language.
 */
public class ExpressionEvaluator
{
    private final SymbolTable _symbols;

    public ExpressionEvaluator(SymbolTable symbols)
    {
        _symbols = symbols;
    }

    /**
     * The expression must be type annotated and correct before the
     * interpreter is invoked. Otherwise the interpreter will fail
     * with IllegalArgumentException.
     */
    public Object evaluate(Expression expression)
    {
        switch (expression.getType()) {
        case DOUBLE:
            return evaluateDouble(expression);
        case BOOLEAN:
            return evaluateBoolean(expression);
        case STRING:
            return evaluateString(expression);
        }
        throw new IllegalArgumentException("Expression lacks type annotation");
    }

    /**
     * The expression must be type annotated and correct before the
     * interpreter is invoked. Otherwise the interpreter will fail
     * with IllegalArgumentException.
     */
    public double evaluateDouble(Expression expression)
    {
        switch (expression.getToken()) {
        case NUMBER_LITERAL:
            return expression.getNumber();
        case IDENTIFIER:
            String identifier = expression.getString();
            Symbol value = _symbols.get(identifier);
            if (value == null) {
                throw new IllegalArgumentException("Unknown identifier: " + identifier);
            }
            return (Double) value.getValue();
        case PLUS:
            return
                evaluateDouble(expression.get(0)) +
                evaluateDouble(expression.get(1));
        case MINUS:
            return
                evaluateDouble(expression.get(0)) -
                evaluateDouble(expression.get(1));
        case MULT:
            return
                evaluateDouble(expression.get(0)) *
                evaluateDouble(expression.get(1));
        case DIV:
            return
                evaluateDouble(expression.get(0)) /
                evaluateDouble(expression.get(1));
        case MOD:
            return
                evaluateDouble(expression.get(0)) %
                evaluateDouble(expression.get(1));
        case POWER:
            return Math.pow(evaluateDouble(expression.get(0)),
                            evaluateDouble(expression.get(1)));
        case IF:
            return (evaluateBoolean(expression.get(0))
                    ? evaluateDouble(expression.get(1))
                    : evaluateDouble(expression.get(2)));
        case UMINUS:
            return -evaluateDouble(expression.get(0));
        default:
            throw new IllegalArgumentException("Invalid operator: " + expression.getToken());
        }
    }

    /**
     * The expression must be type annotated and correct before the
     * interpreter is invoked. Otherwise the interpreter will fail
     * with IllegalArgumentException.
     */
    public boolean evaluateBoolean(Expression expression)
    {
        switch (expression.getToken()) {
        case IDENTIFIER:
            String identifier = expression.getString();
            Symbol value = _symbols.get(identifier);
            if (value == null) {
                throw new IllegalArgumentException("Unknown identifier: " + identifier);
            }
            return (Boolean) value.getValue();
        case TRUE:
            return true;
        case FALSE:
            return false;
        case LT:
            return
                evaluateDouble(expression.get(0)) <
                evaluateDouble(expression.get(1));
        case LE:
            return
                evaluateDouble(expression.get(0)) <=
                evaluateDouble(expression.get(1));
        case GT:
            return
                evaluateDouble(expression.get(0)) >
                evaluateDouble(expression.get(1));
        case GE:
            return
                evaluateDouble(expression.get(0)) >=
                evaluateDouble(expression.get(1));
        case EQ:
            switch (expression.get(0).getType()) {
            case DOUBLE:
                return
                    evaluateDouble(expression.get(0)) ==
                    evaluateDouble(expression.get(1));
            case BOOLEAN:
                return
                    evaluateBoolean(expression.get(1)) ==
                    evaluateBoolean(expression.get(1));
            case STRING:
                return
                    evaluateString(expression.get(0)).equals(evaluateString(expression.get(1)));
            }
            throw new IllegalStateException("Expression lacks type annotation");

        case NE:
            switch (expression.get(0).getType()) {
            case DOUBLE:
                return
                    evaluateDouble(expression.get(0)) !=
                    evaluateDouble(expression.get(1));
            case BOOLEAN:
                return
                    evaluateBoolean(expression.get(0)) !=
                    evaluateBoolean(expression.get(1));
            case STRING:
                return
                    !evaluateString(expression.get(0)).equals(evaluateString(expression.get(1)));
            }
            throw new IllegalStateException("Expression lacks type annotation");

        case IF:
            return (evaluateBoolean(expression.get(0))
                    ? evaluateBoolean(expression.get(1))
                    : evaluateBoolean(expression.get(2)));
        case AND:
            return
                evaluateBoolean(expression.get(0)) &&
                evaluateBoolean(expression.get(1));
        case OR:
            return
                evaluateBoolean(expression.get(0)) ||
                evaluateBoolean(expression.get(1));
        case NOT:
            return !evaluateBoolean(expression.get(0));
        case MATCH:
            return evaluateString(expression.get(0)).matches(evaluateString(expression.get(1)));
        case NOT_MATCH:
            return !evaluateString(expression.get(0)).matches(evaluateString(expression.get(1)));
        default:
            throw new IllegalArgumentException("Invalid operator: " +
                                               expression.getToken());
        }
    }

    public String evaluateString(Expression expression)
    {
        switch (expression.getToken()) {
        case STRING_LITERAL:
            return expression.getString();
        case IDENTIFIER:
            String identifier = expression.getString();
            Symbol value = _symbols.get(identifier);
            if (value == null) {
                throw new IllegalArgumentException("Unknown identifier: " + identifier);
            }
            return (String) value.getValue();
        case IF:
            return (evaluateBoolean(expression.get(0))
                    ? evaluateString(expression.get(1))
                    : evaluateString(expression.get(2)));
        default:
            throw new IllegalArgumentException("Invalid operator: " +
                                               expression.getToken());
        }
    }
}
