package com.joe.blog_java_test.utils;

import java.io.File;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public class PathNameUtil
{
    private static final String _DT_FILE_DATE = "(dt=fileDate)";
    private static final String _FILE_NAME = "(fileName)";
    private static final String _FILE_DATE = "(fileDate)";
    private static final String _SYSTEM_MODE = "(systemMode)";

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

    /**
     * (fileName)/(dt=fileDate)/{SOURCE}_(fileName)_(fileDate).{EXTENSION}
     *
     * @param fileName
     * @return
     */
    public String modifyFolderPath(String beforePath, String fileName)
    {
        if (beforePath == null || beforePath.isEmpty())
        {
            return beforePath;
        }
        String nowPath;
        FileNameUtil fileNameUtil = new FileNameUtil();
        nowPath = judgeSlash(beforePath
                .replace(_DT_FILE_DATE, "dt=" + fileNameUtil.getFileDate(fileName))
                .replace(_FILE_NAME, fileNameUtil.getFolderNameByFileName(fileName))
        );

        return nowPath;
    }
 /**
 * (systemMode)/(fileDate)/{SOURCE}_(fileName)_(fileDate).{EXTENSION}
 *
 * @param
 * @return
 */
public String modifyFolderPath(String beforePath, LocalDate fileDate, String systemMode)
{
    if (beforePath == null || beforePath.isEmpty())
    {
        return beforePath;
    }
    DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyyMMdd");
    String fileDateStr = fileDate.format(format);
    String nowPath;
    nowPath = judgeSlash(beforePath
            .replace(_FILE_DATE, fileDateStr)
            .replace(_SYSTEM_MODE, systemMode)
    );

    return nowPath;
}

}
