package com.joe.blog_java_test.utils;

import java.io.File;
import java.util.Objects;

public class PathNameUtil
{
    /**
     * judge slash
     *
     * @return
     */
    public static String modifyFolderPath(String path)
    {
        if (path == null || path.isEmpty())
        {
            return "";
        }
        return judgeSlash(path);
    }

    private static String judgeSlash(String path)
    {
        String s = Objects.equals(path.substring(path.length() - 1), File.separator) ? path : path + File.separator;
        return s.replace(File.separator + File.separator, File.separator);
    }
}
