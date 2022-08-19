package com.nexr.spark.action.app.conf;

public class JavaWordCountFmt {

    enum TestFmt {
        SQUARE,
        COMMA,
        PARENTHESES
    }

    private static final String PRINTOUT_FORMATTER_SQUARE = "[%s=%s] ";
    private static final String PRINTOUT_FORMATTER_COMMA = "%s=%s, ";
    private static final String PRINTOUT_FORMATTER_PARENTHESES = "(%s=%s) ";
    private static final String PRINTOUT_FORMATTER_DEFAULT = "%s=%s ";

    public static String getPrintOutFormat(String name) {
        TestFmt format = TestFmt.valueOf(name);

        switch (format) {
            case SQUARE:
                return PRINTOUT_FORMATTER_SQUARE;
            case COMMA:
                return PRINTOUT_FORMATTER_COMMA;
            case PARENTHESES:
                return PRINTOUT_FORMATTER_PARENTHESES;
            default:
                return PRINTOUT_FORMATTER_DEFAULT;
        }

    }
}
