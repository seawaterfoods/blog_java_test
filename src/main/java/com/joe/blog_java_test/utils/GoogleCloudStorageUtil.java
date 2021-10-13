package com.joe.blog_java_test.utils;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.*;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

@Slf4j
public class GoogleCloudStorageUtil
{
    private Storage storage;
    private final String jsonPath;

    public GoogleCloudStorageUtil(String jsonPath)
    {
        this.jsonPath = jsonPath;
    }

    public boolean connectToStorage()
    {
        try
        {
            GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(jsonPath))
                    .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));

            this.storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
        } catch (Exception e)
        {
            log.error("[{}]:[{}]", e, e.getMessage());
            return true;
        }
        return false;
    }

    public Storage getStorage()
    {
        return storage;
    }


    public boolean downloadBucketFileToLocal(String localPath, String bucketName, String regex, String remotePath)
    {
//        log.info("downloadBucketFileToLocal(localPath:{}, bucketName:{}, regex:{}, remotePath:{})", localPath, bucketName, regex, remotePath);
        if (connectToStorage())
            return false;
        String fromPath = PathNameUtil.modifyFolderPath(remotePath);
        String toPath = PathNameUtil.modifyFolderPath(localPath);
        LocalFileUtil.checkFolderExist(toPath);
        Page<Blob> blobs =
                storage.list(
                        bucketName,
                        Storage.BlobListOption.prefix(fromPath),
                        Storage.BlobListOption.currentDirectory());
        Pattern pattern = Pattern.compile(regex);
        for (Blob blob : blobs.iterateAll())
        {
            String objectName = blob.getName().replace(fromPath, "");
            if (objectName.equals(""))
                continue;
            String localOutputObject = toPath + objectName;
//            log.info("Bucket:{}, blob.getName():{}, objectName:{}",blob.getBucket(),blob.getName(),objectName);
            if (pattern.matcher(objectName).matches())
            {
                blob.downloadTo(Paths.get(localOutputObject));
                log.info("From bucketName:[{}] fromPath:[{}] Downloaded file:[{}] to localOutputObject:[{}]", bucketName, fromPath, objectName, localOutputObject);
                storage.delete(bucketName, blob.getName());
            }
        }
        return true;
    }

    public boolean uploadBucketFileToLocal(String localPath, String bucketName, String regex, String remotePath) throws IOException
    {
//        log.info("uploadBucketFileToLocal(localPath:{}, bucketName:{}, regex:{}, remotePath:{})", localPath, bucketName, regex, remotePath);
        if (connectToStorage())
            return false;
        String fromPath = PathNameUtil.modifyFolderPath(localPath);
        String toPath = PathNameUtil.modifyFolderPath(remotePath);
        LocalFileUtil.checkFolderExist(fromPath);
        File[] fileArray = new File(fromPath).listFiles();
        Pattern pattern = Pattern.compile(regex);

        if (fileArray == null)
            return false;
        for (File currentFile : fileArray)
        {
            if (!currentFile.isDirectory() && pattern.matcher(currentFile.getName()).matches())
            {
                String localInputObject = fromPath + currentFile.getName();
                String GCSInputObject = !toPath.isEmpty() ? toPath + currentFile.getName() : currentFile.getName();
                if (currentFile.exists())
                {
                    BlobId inBlobId = BlobId.of(bucketName, GCSInputObject);
                    BlobInfo blobInfo = BlobInfo.newBuilder(inBlobId).build();
                    storage.create(blobInfo, Files.readAllBytes(Paths.get(localInputObject)));
                    log.info("Target bucketName:[{}] fromPath:[{}] Uploaded file:[{}] To GCSInputObject:[{}]",
                            bucketName, fromPath, currentFile.getName(), GCSInputObject);

                    if (!currentFile.delete())
                        log.error("File:[{}] delete fail.", currentFile.getName());
                }
            }
        }
        return true;
    }

    public String getFileSourceUrl(Blob blob) throws IOException
    {
        URL signedUrl = blob.signUrl(1, TimeUnit.HOURS, Storage.SignUrlOption.signWith(
                ServiceAccountCredentials.fromStream(new FileInputStream(jsonPath))));
        return signedUrl.toString();
    }
}
