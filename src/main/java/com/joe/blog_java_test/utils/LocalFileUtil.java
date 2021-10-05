package com.joe.blog_java_test.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

@Slf4j
public class LocalFileUtil
{
    FileNameUtil fileNameUtil;

    public LocalFileUtil()
    {
        if (this.fileNameUtil == null)
            this.fileNameUtil = new FileNameUtil();
    }

    public void copyFile(String sourceFilePath, String destFolderPath, String doneFolder, String targetRegex, String ignoreRegex) throws Exception
    {
        String fileName = "";
        try
        {
            File sourceFile = new File(sourceFilePath);
            if (!sourceFile.isDirectory())
            {//判斷是否為資料夾或檔案 若為檔案:
                fileName = sourceFile.getName();
                if (!isIgnoreFile(fileName, ignoreRegex))
                {//判斷是否為需要忽略的程式
                    if (isTargetFile(fileName, targetRegex))
                    {//判斷是否為被標記的程式
                        if (copyFileToLocal(sourceFile, destFolderPath + "/" + fileName))
                        {//複製到destFolderPath
                            if (copyFileToLocal(sourceFile, sourceFile.getAbsolutePath() + "/" + doneFolder + "/" + fileName))
                            {//複製到Local的doneFolder中
                                sourceFile.delete();
                            }
                        }
                    }
                }
            } else
            {//判斷是否為資料夾或檔案 若為資料夾:
                for (String sourceName : sourceFile.list())
                {
                    File sourceChildFile = new File(sourceFile.getAbsolutePath() + "/" + sourceName);
                    if (!sourceChildFile.isDirectory())
                    {
                        fileName = sourceName;
                        if (!isIgnoreFile(fileName, ignoreRegex))
                        {
                            if (isTargetFile(fileName, targetRegex))
                            {
                                if (copyFileToLocal(sourceChildFile, destFolderPath + "/" + sourceName))
                                {
                                    if (copyFileToLocal(sourceChildFile, sourceFile.getAbsolutePath() + "/" + doneFolder + "/" + sourceName))
                                    {
                                        sourceChildFile.delete();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e)
        {
            throw new Exception("Copy Local file Failure : " + sourceFilePath + ", Exception : " + e.getMessage());
        }

    }

    private Boolean copyFileToLocal(File sourceFile, String copyFilePath) throws Exception
    {
        Boolean result = true;
        File copyFile = new File(copyFilePath);
        try
        {

            checkFolderExist(copyFilePath);
            copyContent(sourceFile, copyFile);

        } catch (Exception e)
        {
            result = false;
            throw new Exception("Copy Local file Failure : " + sourceFile.getName() + ", Exception : " + e.getMessage());
        }
        return result;
    }

    public static void checkFolderExist(String filePath)
    {

        String folderPath = filePath.substring(0, filePath.lastIndexOf(File.separator));
        File folder = new File(folderPath);
        if (!folder.exists())
        {
            folder.mkdir();
//            System.out.println("new folder Path:" + folderPath);
        }
    }

    public static void copyContent(File targetFile, File desFile)
            throws Exception
    {
        Path sourcePath      = Paths.get(targetFile.getPath());
        Path destinationPath = Paths.get(desFile.getPath());

        try {
            Files.copy(sourcePath, destinationPath);
        } catch(FileAlreadyExistsException e) {
            //destination file already exists
        } catch (IOException e) {
            //something else went wrong
            throw e;
        }
    }

    private Boolean isTargetFile(String fileName, String targetRegex)
    {

        Boolean result = false;

        if (null == targetRegex || "".equals(targetRegex))
        {
            result = true;
        } else
        {
            String[] regexs = targetRegex.split(",");
            for (String regex : regexs)
            {
                Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);

//                String fileExtansion = "." + fileName.substring(fileName.lastIndexOf(".")+1);
//                if(fileExtansion.equals(extansion)){
                if (pattern.matcher(fileName).find())
                {
                    result = true;
                    break;
                }
            }
        }

        return result;

    }

    private Boolean isIgnoreFile(String fileName, String ignoreRegex)
    {

        Boolean result = true;

        if (null == ignoreRegex || "".equals(ignoreRegex))
        {
            result = false;
        } else
        {
            String[] regexs = ignoreRegex.split(",");
            for (String regex : regexs)
            {
                Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);

//                String fileExtansion = "." + fileName.substring(fileName.lastIndexOf(".")+1);
//                if(fileExtansion.equals(extansion)){
                if (pattern.matcher(fileName).find())
                {
//                    result = true;
                    break;
                }
            }
        }

        return result;

    }

    public void suffixRename(String localOutputFilePath, String renameStr, String regex)
    {
        checkFolderExist(localOutputFilePath + "/");
        File[] fileArray = new File(localOutputFilePath).listFiles();
        String repaceStr = renameStr;

        for (File currentFile : fileArray)
        {
            Pattern pattern = Pattern.compile(regex);
            if (!currentFile.isDirectory() && pattern.matcher(currentFile.getName()).matches())
            {
                try
                {
                    File file = new File(currentFile.getAbsolutePath());
                    String oldSuffix = fileNameUtil.getSuffixName(file.getName());
                    String newSuffix = oldSuffix.replace(repaceStr, "");
                    String oldName = file.getName();
                    String newFileName = oldName.replace(oldSuffix, newSuffix);
                    File fileRename = new File(localOutputFilePath + File.separator + newFileName);
                    file.renameTo(fileRename);

//                    log.info("suffixRename oldName:{}, oldSuffix:{}, newSuffix:{}, newFileName:{}", oldName, oldSuffix, newSuffix, newFileName);
                } catch (Exception e)
                {
//                    log.error("suffixRename currentFile.getName():{} {}: {}", currentFile.getName(),e, e.getMessage());
//                    e.printStackTrace();
                    throw e;
                }

            }
        }
    }

    public void prefixRename(String LocalInputFilePath, String renameStr, String regex)
    {
        checkFolderExist(LocalInputFilePath + "/");
        File[] fileArray = new File(LocalInputFilePath).listFiles();

        for (File currentFile : fileArray)
        {

            Pattern pattern = Pattern.compile(regex);
            if (!currentFile.isDirectory() && pattern.matcher(currentFile.getName()).matches())
            {
                try
                {
                    File file = new File(currentFile.getAbsolutePath());
                    String oldPrefix = fileNameUtil.getPrefixName(file.getName());
                    String newPrefix = renameStr != null ? renameStr + oldPrefix : oldPrefix;
                    String oldName = file.getName();
                    String newFileName = oldName.replace(oldPrefix, newPrefix);
                    File fileRename = new File(LocalInputFilePath + File.separator + newFileName);
                    file.renameTo(fileRename);

//                    log.info("prefixRename oldName:{}, oldPrefix:{}, addPrefix:{}, newPrefix:{}, newFileName:{}", oldName, oldPrefix, renameStr, newPrefix, newFileName);
                } catch (Exception e)
                {
//                    log.error("prefixRename currentFile.getName():{} {}: {}", currentFile.getName(),e, e.getMessage());
//                    e.printStackTrace();
                    throw e;
                }

            }
        }
    }

    public void dateToExtensionStrRemove(String LocalInputFilePath, String regex)
{
    checkFolderExist(LocalInputFilePath + "/");
    File[] fileArray = new File(LocalInputFilePath).listFiles();

    for (File currentFile : fileArray)
    {

        Pattern pattern = Pattern.compile(regex);
        if (!currentFile.isDirectory() && pattern.matcher(currentFile.getName()).matches())
        {
            try
            {
                File file = new File(currentFile.getAbsolutePath());
                String fileDateToExtensionStr = fileNameUtil.getFileDateToExtensionStr(file.getName());
                String oldName = file.getName();
                String newFileName = oldName.replace(fileDateToExtensionStr, "");
                File fileRename = new File(LocalInputFilePath + File.separator + newFileName);
                file.renameTo(fileRename);

                    log.info("prefixRename oldName:{}, fileDate:{}, newFileName:{}", oldName, fileDateToExtensionStr, newFileName);
            } catch (Exception e)
            {
//                    log.error("prefixRename currentFile.getName():{} {}: {}", currentFile.getName(),e, e.getMessage());
//                    e.printStackTrace();
                throw e;
            }

        }
    }
}


}
