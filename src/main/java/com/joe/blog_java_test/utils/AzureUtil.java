package com.joe.blog_java_test.utils;

import com.azure.core.util.polling.LongRunningOperationStatus;
import com.azure.core.util.polling.SyncPoller;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobCopyInfo;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.sas.BlobContainerSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.file.share.models.ShareFileCopyInfo;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.regex.Pattern;

@Slf4j
public class AzureUtil
{
    private final String storageConnectionString;
    private String containerName = "";
    private BlobContainerClient blobContainerClient = null;
    public final String _FAIL = "fail";
    public final String _SUCCESS = "success";

    public AzureUtil(String connectionStr)
    {
        this.storageConnectionString = connectionStr;
    }

    //====================================Azure Blob====================================
    /**
     * 連線到另外的Azure Blob中，需提供storageConnectionString與containerName。
     * @return
     */
    private BlobContainerClient connectToBlob(String storageConnectionString, String containerName) throws Exception
    {
        BlobContainerClient result;
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(storageConnectionString).buildClient();
        try
        {
            result = blobServiceClient.getBlobContainerClient(containerName);
            result.exists();
        } catch (Exception e)
        {
            log.error("connect to other blob fail {}:[{}]", e, e.getMessage());
//            e.printStackTrace();
            throw new Exception("connect to other blob fail " + e + ":[" + e.getMessage() + "]");
        }
        return result;
    }

    /**
     * 連線到Azure Blob中，需提供containerName。
     * @return
     */
    private boolean connectToBlob(String containerName)
    {
        this.containerName = containerName;
        return connectToBlob();
    }

    /**
     * 連線到Azure Blob。
     * @return
     */
    private boolean connectToBlob()
    {
        try
        {
            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(storageConnectionString).buildClient();

            this.blobContainerClient = blobServiceClient.getBlobContainerClient(this.containerName);
            blobContainerClient.exists();
        } catch (Exception e)
        {
            log.error("storageConnectionString fail {}:[{}]", e, e.getMessage());
//            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 批量下載Azure Blob資料到本地端。
     * @param fromSystem
     * @param remotePath
     * @param localPath
     * @param regex
     * @return
     */
    public boolean batchDownloadBlobFilesToLocal(String fromSystem, String remotePath, String localPath, String regex)
    {
        return batchDownloadBlobFilesToLocal(fromSystem, remotePath, localPath, regex, true);
    }

    /**
     * 批量下載Azure Blob資料到本地端，可設定是否要覆蓋本地同名資料。
     * @param fromContainer
     * @param remotePath
     * @param localPath
     * @param regex
     * @param overwrite
     * @return
     */
    public boolean batchDownloadBlobFilesToLocal(String fromContainer, String remotePath, String localPath, String regex, boolean overwrite)
    {
        if (!connectToBlob(fromContainer))
            return false;
        String fromPath = PathNameUtil.modifyFolderPath(remotePath);
        String toPath = PathNameUtil.modifyFolderPath(localPath);
        LocalFileUtil.checkFolderExist(toPath);
        Pattern pattern = Pattern.compile(regex);

        for (BlobItem blobItem : blobContainerClient.listBlobs())
        {
            String blobName = blobItem.getName();
            String fileName = blobName.replace(fromPath, "");
            if (pattern.matcher(fileName).matches() && blobName.contains(fromPath))
            {
                String status = downloadBlobFileToLocal(blobItem, fileName, localPath, overwrite) ? _SUCCESS : _FAIL;
                log.info("From Blob Container:[{}] Download Blob File:[{}] To localObject:[{}] Status:[{}]", fromContainer, blobName, toPath + fileName, status);
            }
        }
        return true;
    }

    /**
     * 下載Blob單一資料到本地端。
     * @param blobItem
     * @param fileName
     * @param localPath
     * @param overwrite
     * @return
     */
    private boolean downloadBlobFileToLocal(BlobItem blobItem, String fileName, String localPath, boolean overwrite)
    {
        try
        {
            BlobClient sourceClient = blobContainerClient.getBlobClient(blobItem.getName());
            sourceClient.downloadToFile(localPath + fileName, overwrite);
            sourceClient.delete();
        } catch (Exception e)
        {
            log.error("downloadBlobFile:[{}] fail [{}]:[{}]", blobItem.getName(), e, e.getMessage());
            return false;
        }
        return true;
    }


    /**
     * 批量上傳本地端資料到Azure Blob。
     * @param toContainer
     * @param remotePath
     * @param localPath
     * @param regex
     * @return
     */
    public boolean batchUploadLocalFilesToBlob(String toContainer, String remotePath, String localPath, String regex)
    {
        if (!connectToBlob(toContainer))
            return false;
        String toPath = PathNameUtil.modifyFolderPath(remotePath);
        String fromPath = PathNameUtil.modifyFolderPath(localPath);
        LocalFileUtil.checkFolderExist(fromPath);
        File fileDir = new File(fromPath);
        File sourceFile;
        Pattern pattern = Pattern.compile(regex);

        if (!fileDir.isDirectory())
            return false;

        for (String fileName : Objects.requireNonNull(fileDir.list()))
        {
            if (!pattern.matcher(fileName).matches())
                continue;
            sourceFile = new File(fromPath + fileName);
            if (sourceFile.isDirectory())
                continue;
            BlobClient sourceBlob = blobContainerClient.getBlobClient(toPath + fileName);
            sourceBlob.uploadFromFile(sourceFile.getAbsolutePath());
            log.info("To Blob Container:[{}]  Uploaded localFile:[{}] To blobInputObject:[{}]", toContainer, sourceFile.getAbsolutePath(), sourceBlob.getBlobName());
            sourceFile.delete();
        }
        return true;
    }

    /**
     * 批量移動Azure Blob資料到其他Azure Blob。
     * @param fromContainer
     * @param remoteFromPath
     * @param toStorageConnectionString
     * @param toContainer
     * @param remoteToPath
     * @param regex
     * @return
     * @throws Exception
     */
    public Boolean batchMoveBlobFileToBlob(String fromContainer, String remoteFromPath, String toStorageConnectionString, String toContainer, String remoteToPath, String regex) throws Exception
    {
        if (!connectToBlob(fromContainer))
            return false;
        BlobContainerClient toBlobContainerClient = connectToBlob(toStorageConnectionString, toContainer);
        String fromPath = PathNameUtil.modifyFolderPath(remoteFromPath);
        String toPath = PathNameUtil.modifyFolderPath(remoteToPath);
        Pattern pattern = Pattern.compile(regex);
        Map<SyncPoller<BlobCopyInfo, Void>, BlobClient> infoMap = new HashMap<>();

        for (BlobItem blobItem : blobContainerClient.listBlobs())
        {
            String blobName = blobItem.getName();
            String fileName = blobName.replace(fromPath, "");
            if (!pattern.matcher(fileName).matches())
                continue;
            BlobClient fromClient = blobContainerClient.getBlobClient(blobName);
            BlobClient toClient = toBlobContainerClient.getBlobClient(toPath + fileName);
            SyncPoller<BlobCopyInfo, Void> info = toClient.beginCopy(getBlobFileSourceUrl(fromClient), null);

            infoMap.put(info, fromClient);
        }
        for (SyncPoller<BlobCopyInfo, Void> info : infoMap.keySet())
        {
            while (info.poll().getStatus() == LongRunningOperationStatus.IN_PROGRESS)
            {
                Thread.sleep(500);
            }
            if ((info.poll().getStatus() == LongRunningOperationStatus.SUCCESSFULLY_COMPLETED))
                infoMap.get(info).delete();
        }
        return true;
    }

    /**
     * 取得Azure Blob資料下載的Url。
     * @param fromBlob
     * @return
     */
    public String getBlobFileSourceUrl(BlobClient fromBlob)
    {
        BlobServiceSasSignatureValues sas = new BlobServiceSasSignatureValues(OffsetDateTime.now().plusHours(1), BlobContainerSasPermission.parse("r"));
        String sasToken = fromBlob.generateSas(sas);
        return fromBlob.getBlobUrl() + "?" + sasToken;
    }


}
