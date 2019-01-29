package com.thebeastshop.liteflow.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by gongsiran on 28/1/2019.
 */
public class PatternUtil {
    private  static String zKConfigPattern="[\\w\\d][\\w\\d\\.]+\\:(\\d)+(\\,[\\w\\d][\\w\\d\\.]+\\:(\\d)+)*";

    private  static String localConfigPattern="^[\\w\\/]+(\\/\\w+)*\\.xml$";

    private  static String classConfigPattern="^\\w+(\\.\\w+)*$";

    private  static String parseNodeStr="[^\\)\\(]+";


    public static boolean isZKConfig(String path) {
        Pattern p = Pattern.compile(zKConfigPattern);
        Matcher m = p.matcher(path);
        return m.find();
    }

    public static boolean isLocalConfig(String path) {
        Pattern p = Pattern.compile(localConfigPattern);
        Matcher m = p.matcher(path);
        return m.find();
    }

    public static boolean isClassConfig(String path) {
        Pattern p = Pattern.compile(classConfigPattern);
        Matcher m = p.matcher(path);
        return m.find();
    }
    public static Matcher parseNodeStr(String path) {
        Pattern p = Pattern.compile(parseNodeStr);
        return p.matcher(path);
    }


}
