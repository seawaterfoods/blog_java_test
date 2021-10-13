package com.joe.blog_java_test.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.File;

@Slf4j
public class LocalFileUtil
{
    public static void checkFolderExist(String filePath)
    {

        String folderPath = filePath.substring(0, filePath.lastIndexOf(File.separator));
        File folder = new File(folderPath);
        if (!folder.exists() && folder.mkdir())
        {
            log.info("new folder Path:[{}]" + folderPath);
        }
    }

    public static boolean checkFileExist(String filePath)
    {
        File result = new File(filePath);
        return result.exists() && result.isFile();
    }


}
