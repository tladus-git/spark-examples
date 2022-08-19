package com.nexr.spark.action.app.conf;

public class JavaWordCountSeparator {

    enum Separator {
        SPACE,
        COMMA,
        SEMICOLON,
        COLON
    }

    public static String getSeparator(String name) {
        Separator separator = Separator.valueOf(name);

        switch(separator) {
            case COMMA:
                return ",";
            case SEMICOLON:
                return ";";
            case COLON:
                return ":";
            case SPACE:
            default:
                return " ";
        }

    }
}
