package com.joe.java_test.utils;

import com.azure.core.http.rest.PagedIterable;
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
import com.azure.storage.file.share.ShareClient;
import com.azure.storage.file.share.ShareClientBuilder;
import com.azure.storage.file.share.ShareFileClient;
import com.azure.storage.file.share.models.ShareFileCopyInfo;
import com.azure.storage.file.share.models.ShareFileItem;
import com.azure.storage.file.share.sas.ShareFileSasPermission;
import com.azure.storage.file.share.sas.ShareServiceSasSignatureValues;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

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
    private ShareClient fileShareContainerClient = null;
    public final String _FAIL = "fail";
    public final String _SUCCESS = "success";
    private final int timeoutSec = 600;

    public AzureUtil(String connectionStr)
    {
        this.storageConnectionString = connectionStr;
    }

    //====================================Azure Blob====================================

    /**
     * 連線到另外的Azure Blob中，需提供storageConnectionString與containerName。
     */
    public BlobContainerClient connectToBlob(String storageConnectionString, String containerName) throws Exception
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
     */
    private boolean connectToBlob(String containerName)
    {
        this.containerName = containerName;
        return connectToBlob();
    }

    /**
     * 連線到Azure Blob。
     */
    private boolean connectToBlob()
    {
        try
        {
            this.blobContainerClient = new BlobServiceClientBuilder()
                    .connectionString(storageConnectionString).buildClient()
                    .getBlobContainerClient(this.containerName);
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
     */
    public boolean batchDownloadBlobFilesToLocal(String sourceContainer, String remotePath, String localPath, String regex)
    {
        return batchDownloadBlobFilesToLocal(sourceContainer, remotePath, localPath, regex, true);
    }

    /**
     * 批量下載Azure Blob資料到本地端，可設定是否要覆蓋本地同名資料。
     */
    public boolean batchDownloadBlobFilesToLocal(String sourceContainer, String remotePath, String localPath, String regex, boolean overwrite)
    {
        if (!connectToBlob(sourceContainer))
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
                log.info("Source Blob Container:[{}] Download Blob File:[{}] To localObject:[{}] Status:[{}]", sourceContainer, blobName, toPath + fileName, status);
            }
        }
        return true;
    }

    /**
     * 下載Blob單一資料到本地端。
     */
    private boolean downloadBlobFileToLocal(BlobItem blobItem, String fileName, String localPath, boolean overwrite)
    {
        try
        {
            BlobClient sourceClient = blobContainerClient.getBlobClient(blobItem.getName());
            if (overwrite)
            {
                sourceClient.downloadToFile(localPath + fileName, true);
            } else
            {
                if (LocalFileUtil.checkFileExist(localPath + fileName))
                    sourceClient.downloadToFile(localPath + fileName);
            }
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
     */
    public boolean batchUploadLocalFilesToBlob(String targetContainer, String remotePath, String localPath, String regex)
    {
        if (!connectToBlob(targetContainer))
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
            log.info("Target Blob Container:[{}]  Uploaded localFile:[{}] To blobInputObject:[{}]", targetContainer, sourceFile.getAbsolutePath(), sourceBlob.getBlobName());
            if (!sourceFile.delete())
                log.error("File:[{}] delete fail.", sourceFile.getName());
        }
        return true;
    }

    /**
     * 批量移動Azure Blob資料到Azure Blob。
     */
    public Boolean batchMoveBlobFileToBlob(String sourceContainer, String remoteFromPath, String toStorageConnectionString, String targetContainer, String remoteToPath, String regex) throws Exception
    {
        if (!connectToBlob(sourceContainer))
            return false;
        BlobContainerClient targetBlobContainerClient = connectToBlob(toStorageConnectionString, targetContainer);
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
            BlobClient toClient = targetBlobContainerClient.getBlobClient(toPath + fileName);
            SyncPoller<BlobCopyInfo, Void> info = toClient.beginCopy(getBlobFileSourceUrl(fromClient), null);

            log.info("Source Blob Container:[{}] Target Blob Container:[{}] blobOutputObject:[{}] By Source Url:[{}]",
                    fromClient.getBlobName(), toClient.getBlobName(), toPath + fileName, getBlobFileSourceUrl(fromClient));
            infoMap.put(info, fromClient);
        }
        for (SyncPoller<BlobCopyInfo, Void> info : infoMap.keySet())
        {
            int timeout = timeoutSec * 1000;
            while (info.poll().getStatus() == LongRunningOperationStatus.IN_PROGRESS && timeout > 0)
            {
                Thread.sleep(500);
                timeout -= 500;
            }
            if ((info.poll().getStatus() == LongRunningOperationStatus.SUCCESSFULLY_COMPLETED))
                infoMap.get(info).delete();
        }
        return true;
    }

    /**
     * 取得Azure Blob資料下載的Url。
     */
    public String getBlobFileSourceUrl(BlobClient fromBlob)
    {
        BlobServiceSasSignatureValues sas = new BlobServiceSasSignatureValues(OffsetDateTime.now().plusHours(1), BlobContainerSasPermission.parse("r"));
        String sasToken = fromBlob.generateSas(sas);
        return fromBlob.getBlobUrl() + "?" + sasToken;
    }


    //====================================Azure File Share====================================

    /**
     * 連線到另外的Azure File Share中，需提供storageConnectionString與containerName。
     */
    public ShareClient connectToFileShare(String storageConnectionString, String containerName) throws Exception
    {
        ShareClient result;

        try
        {
            result = new ShareClientBuilder()
                    .connectionString(storageConnectionString)
                    .shareName(containerName).buildClient();
            result.exists();
        } catch (Exception e)
        {
            log.error("connect to other file share fail [{}]", e.getMessage());
//            e.printStackTrace();
            throw new Exception("connect to other file share fail " + e + ":[" + e.getMessage() + "]");
        }
        return result;
    }


    /**
     * 連線到Azure File Share。
     */
    private boolean connectToFileShare()
    {
        try
        {
            this.fileShareContainerClient = new ShareClientBuilder()
                    .connectionString(storageConnectionString)
                    .shareName(containerName).buildClient();
            fileShareContainerClient.exists();
        } catch (Exception e)
        {
            log.error("storageConnectionString fail {}:[{}]", e, e.getMessage());
//            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 連線到Azure File Share，需提供containerName。
     */
    private boolean connectToFileShare(String containerName)
    {
        this.containerName = containerName;
        return connectToFileShare();
    }

    /**
     * 批量下載Azure File Share資料到本地端。
     */
    public boolean batchDownloadFileShareFilesToLocal(String sourceContainer, String remotePath, String localPath, String regex)
    {
        return batchDownloadFileShareFilesToLocal(sourceContainer, remotePath, localPath, regex, true);
    }

    /**
     * 批量下載Azure File Share資料到本地端，可設定是否要覆蓋本地同名資料。
     */
    public boolean batchDownloadFileShareFilesToLocal(String sourceContainer, String remotePath, String localPath, String regex, boolean overwrite)
    {
        if (!connectToFileShare(sourceContainer))
            return false;
        String fromPath = PathNameUtil.modifyFolderPath(remotePath);
        String toPath = PathNameUtil.modifyFolderPath(localPath);
        LocalFileUtil.checkFolderExist(toPath);
        Pattern pattern = Pattern.compile(regex);
        for (ShareFileItem fileRef : getShareFileItem(fromPath))
        {
            String fileName = fileRef.getName();
            if (!pattern.matcher(fileName).matches())
                continue;
            ShareFileClient sourceClient = fileShareContainerClient.getDirectoryClient(StringUtils.removeEnd(fromPath, File.separator)).getFileClient(fileRef.getName());
            String status = downloadFileShareFileToLocal(sourceClient, toPath, fileName, overwrite) ? _SUCCESS : _FAIL;
            log.info("Source FileShare Container:[{}] fromPath:[{}] Download File Name:[{}] To localObject:[{}] Status:[{}]", sourceContainer, fromPath, fileName, toPath + fileName, status);

        }
        return true;
    }

    /**
     * 下載File Share單一資料到本地端。
     */
    private boolean downloadFileShareFileToLocal(ShareFileClient sourceClient, String localPath, String fileName, boolean overwrite)
    {
        try
        {
            if (overwrite)
            {
                if (LocalFileUtil.checkFileExist(localPath + fileName))
                {
                    File sourceFile = new File(localPath + fileName);
                    if (!sourceFile.delete())
                        log.error("File:[{}] delete fail.", sourceFile.getName());
                    log.warn("localFile:[{}] is exist, overwrite this file.", localPath + fileName);
                }
                sourceClient.downloadToFile(localPath + fileName);
                sourceClient.delete();
            } else
            {
                if (LocalFileUtil.checkFileExist(localPath + fileName))
                {
                    log.warn("localFile:[{}] is exist, pass this file.", localPath + fileName);
                } else
                {
                    sourceClient.downloadToFile(localPath + fileName);
                    sourceClient.delete();
                }
            }
        } catch (Exception e)
        {
            log.error("downloadFileShareFile:[{}] fail [{}]:[{}]", sourceClient.getFilePath(), e, e.getMessage());
            return false;
        }
        return true;
    }

    private PagedIterable<ShareFileItem> getShareFileItem(String fromPath)
    {
        return fileShareContainerClient.getDirectoryClient(StringUtils.removeEnd(fromPath, File.separator)).listFilesAndDirectories();
    }

    /**
     * 批量上傳本地端資料到Azure File Share。
     */
    public boolean batchUploadLocalFilesToFileShare(String targetContainer, String remotePath, String localPath, String regex)
    {
        if (!connectToFileShare(targetContainer))
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
            ShareFileClient sourceFileShareClient = fileShareContainerClient.getDirectoryClient(StringUtils.removeEnd(toPath, File.separator)).getFileClient(sourceFile.getName());
            log.info("To Target FileShare Container:[{}] Uploaded localFile:[{}] To fileShareInputObject:[{}]", targetContainer, sourceFile.getAbsolutePath(), sourceFile.getAbsolutePath());
            if (sourceFileShareClient.exists())
                log.warn("fileShareInputObject:[{}] is exist, overwrite this file.", sourceFile.getAbsolutePath());
            sourceFileShareClient.create(sourceFile.length());
            sourceFileShareClient.uploadFromFile(sourceFile.getAbsolutePath());

            if (!sourceFile.delete())
                log.error("File:[{}] delete fail.", sourceFile.getName());
        }
        return true;
    }

    /**
     * 批量移動Azure File Share資料到Azure File Share。
     */
    public Boolean batchMoveFileShareFileToFileShare(String sourceContainer, String remoteFromPath, String toStorageConnectionString, String targetContainer, String remoteToPath, String regex) throws Exception
    {
        if (!connectToFileShare(sourceContainer))
            return false;
        ShareClient targetFileShareContainerClient = connectToFileShare(toStorageConnectionString, targetContainer);
        String fromPath = PathNameUtil.modifyFolderPath(remoteFromPath);
        String toPath = PathNameUtil.modifyFolderPath(remoteToPath);
        Pattern pattern = Pattern.compile(regex);
        Map<SyncPoller<ShareFileCopyInfo, Void>, ShareFileClient> infoMap = new HashMap<>();

        for (ShareFileItem fileShareItem : fileShareContainerClient.getDirectoryClient(StringUtils.removeEnd(fromPath, File.separator)).listFilesAndDirectories())
        {
            String fileShareName = fileShareItem.getName();
            String fileName = fileShareName.replace(fromPath, "");
            if (!pattern.matcher(fileName).matches())
                continue;

            ShareFileClient fromClient = fileShareContainerClient.getDirectoryClient(StringUtils.removeEnd(fromPath, File.separator)).getFileClient(fileShareName);

            ShareFileClient toClient = targetFileShareContainerClient.getFileClient(toPath + fileName);
            System.out.println(getFileShareFileSourceUrl(fromClient));
            SyncPoller<ShareFileCopyInfo, Void> info = toClient.beginCopy(getFileShareFileSourceUrl(fromClient), null, null);

            log.info("Source File Share Container:[{}] Target File Share Container:[{}] FileShareOutputObject:[{}] By Source Url:[{}]",
                    fromClient.getShareName(), toClient.getShareName(), toPath + fileName, getFileShareFileSourceUrl(fromClient));

            infoMap.put(info, fromClient);
        }
        for (SyncPoller<ShareFileCopyInfo, Void> info : infoMap.keySet())
        {
            int timeout = timeoutSec * 1000;
            while (info.poll().getStatus() == LongRunningOperationStatus.IN_PROGRESS && timeout > 0)
            {
                Thread.sleep(500);
                timeout -= 500;
            }
            if ((info.poll().getStatus() == LongRunningOperationStatus.SUCCESSFULLY_COMPLETED))
                infoMap.get(info).delete();
        }
        return true;
    }

    /**
     * 取得Azure File Share資料下載的Url。
     */
    public String getFileShareFileSourceUrl(ShareFileClient fromFileShare)
    {
        ShareServiceSasSignatureValues sas = new ShareServiceSasSignatureValues(OffsetDateTime.now().plusHours(1), ShareFileSasPermission.parse("r"));
        String sasToken = fromFileShare.generateSas(sas);
        return fromFileShare.getFileUrl() + "?" + sasToken;
    }

}
