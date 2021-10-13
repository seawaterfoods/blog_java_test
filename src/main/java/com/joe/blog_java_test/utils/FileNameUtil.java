package com.joe.blog_java_test.utils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileNameUtil
{
    private static final String _DATE_PATTERN = "[2-3][0-3][0-9]{2}[0-1][0-9][0-3][0-9]";
    private static final String _EXTENSION_PATTERN = "\\.[a-z0-9]+";

    public String getFolderNameByFileName(String fileName)
    {
        String transmissionFileShareName = fileName.split("_")[0] + "_";

        Matcher fileDateMatcher = Pattern.compile("_" + _DATE_PATTERN + ".*").matcher(fileName);
        String fileDateStr = fileDateMatcher.find() ? fileDateMatcher.group(0) : "";

        Matcher extensionMatcher = Pattern.compile(".*").matcher(fileName);
        String extensionStr = extensionMatcher.find() ? extensionMatcher.group(0) : "";

//      remove transmissionFileShareName & transmissionDateName

        String newFileName = fileName
                .replace(transmissionFileShareName, "")
                .replace(fileDateStr, "")
                .replace(extensionStr, "");


        return newFileName;
    }

    public String getFileDate(String fileName)
    {
        Pattern pattern = Pattern.compile(_DATE_PATTERN);
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate now = LocalDate.now();

        Matcher matcher = pattern.matcher(fileName);
        String fileDateStr = matcher.find() ? matcher.group(0) : now.format(format);

        return fileDateStr;

    }
}
