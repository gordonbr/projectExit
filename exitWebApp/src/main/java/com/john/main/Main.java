package com.john.main;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jonathasalves on 22/06/2017.
 */
public class Main {

    private static String file = "/Users/jonathasalves/Programming/nasa_access_log";

    private static String ex = "194.65.16.3 - - [09/Jul/1995:12:37:00 -0400] \"GET /images/NASA-logosmall.gif HTTP/1.0\" 200 786";

    private static String ex2 = "unicomp7.dedu.lala - - [01/Jul/1995:00:00:06 -0400] \"GET /shuttle/countdown/ HTTP/1.0\" 200 3985";

    private static String regEx = "^(\\d{1,3}\\.){3}\\d{1,3} - - \\[(\\d){2}/(\\w){3}/(\\d){4}:(\\d){2}:(\\d){2}:(\\d){2} -0400\\]" +
            " (\".+\") (\\d)+ (\\d)+?";

    private static String regEx2 = "^[\\w]+ - - \\[(\\d){2}/(\\w){3}/(\\d){4}:(\\d){2}:(\\d){2}:(\\d){2} -0400\\]" +
            " (\".+\") (\\d)+ (\\d)+?";

    public static void main(String args[]) throws FileNotFoundException {

        System.out.println(ex.matches(regEx));
        System.out.println(ex2.matches(regEx2));

        Pattern pattern = Pattern.compile("([\\S\\.]+) - - (\\[.*?\\]) (\".+\") (\\d+) (\\d+)");

        Matcher m = pattern.matcher(ex2);

        if (m.find()) {
            System.out.println("Found");
            System.out.println(m.group(1));
            System.out.println(m.group(2));
            System.out.println(m.group(3));
            System.out.println(m.group(4));
            System.out.println(m.group(5));
        }
    }

    public static void reader(String file) {
        try {
            BufferedReader fr = new BufferedReader(new FileReader(file));

            fr.lines().forEach(line -> {
                System.out.println(line + "\n\n");

            });
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
