import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.file.share.ShareClient;
import com.azure.storage.file.share.ShareFileClient;
import com.azure.storage.file.share.models.ShareFileItem;
import com.joe.java_test.utils.AzureUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;


public class AzureTest
{

    String connectionStr = "AccountName=postofficeforlocal;AccountKey=QfBLkyzTtw5BbermMLiRNdh4Kd4ByujtRZM2mJlS7+1uObdDuv4b5obctyii27AuXa85reAeUXEBOsHtk0MJOw==;EndpointSuffix=core.windows.net;DefaultEndpointsProtocol=https";

    String localAzureOutputFilePath = "C:" + File.separator + "test" + File.separator + "bfes" + File.separator + "postoffice" + File.separator + "azure" + File.separator + "output" + File.separator;
    String localAzureInputFilePath = "C:" + File.separator + "test" + File.separator + "bfes" + File.separator + "postoffice" + File.separator + "azure" + File.separator + "input" + File.separator;
    String regex = "^[0-9a-zA-Z]{1,}-[0-9a-zA-Z]*_.*";
    String outputFilePath = "file_import/output/";
    String inputFilePath = "file_import/input/";
    String _TAIPEI = "taipei";
    String _KAOHSIUNG = "kaohsiung";

    @Test
    public void downloadAzureBlobFilesTest()
    {
        AzureUtil azureUtil = new AzureUtil(connectionStr);

        Assertions.assertTrue(azureUtil.batchDownloadBlobFilesToLocal(_TAIPEI, outputFilePath, localAzureOutputFilePath, regex));
    }

    @Test
    public void uploadAzureBlobFilesTest()
    {
        AzureUtil azureUtil = new AzureUtil(connectionStr);

        Assertions.assertTrue(azureUtil.batchUploadLocalFilesToBlob(_TAIPEI, inputFilePath, localAzureInputFilePath, regex));
    }

    @Test
    public void moveBetweenAzureBlobFilesTest() throws Exception
    {
        AzureUtil azureUtil = new AzureUtil(connectionStr);

        Assertions.assertTrue(azureUtil.batchMoveBlobFileToBlob(_TAIPEI, inputFilePath, connectionStr, _KAOHSIUNG, outputFilePath, regex));
    }

    @Test
    public void downloadAzureFileShareFilesTest()
    {
        AzureUtil azureUtil = new AzureUtil(connectionStr);

        Assertions.assertTrue(azureUtil.batchDownloadFileShareFilesToLocal(_TAIPEI, outputFilePath, localAzureOutputFilePath, regex));
    }

    @Test
    public void uploadAzureFileShareFilesTest()
    {
        AzureUtil azureUtil = new AzureUtil(connectionStr);

        Assertions.assertTrue(azureUtil.batchUploadLocalFilesToFileShare(_TAIPEI, inputFilePath, localAzureInputFilePath, regex));
    }

    @Test
    public void moveBetweenAzureFileShareFilesTest() throws Exception
    {
        AzureUtil azureUtil = new AzureUtil(connectionStr);

        Assertions.assertTrue(azureUtil.batchMoveFileShareFileToFileShare(_TAIPEI, inputFilePath, connectionStr, _KAOHSIUNG, outputFilePath, regex));
    }

    public void azureSourceUrlTest() throws Exception
    {
//        Blob to file-share, or file-share to blob cannot directly use the SDK to transfer files. Their certifications do not match.
        AzureUtil azureUtil = new AzureUtil(connectionStr);
//        BlobContainerClient blobContainerClient = azureUtil.connectToBlob(connectionStr,_TAIPEI);
        ShareClient shareClient = azureUtil.connectToFileShare(connectionStr, _TAIPEI);

//        ShareClient targetFileShareContainerClient = azureUtil.connectToFileShare(connectionStr, _KAOHSIUNG);
        for (ShareFileItem fileShareItem : shareClient.getDirectoryClient(StringUtils.removeEnd(inputFilePath, "/")).listFilesAndDirectories())
        {
            String fileName = fileShareItem.getName();
            ShareFileClient fromClient = shareClient.getDirectoryClient(StringUtils.removeEnd(inputFilePath, "/")).getFileClient(fileName);

            BlobContainerClient blobContainerClient = azureUtil.connectToBlob(connectionStr, _KAOHSIUNG);
            BlobClient toClient = blobContainerClient.getBlobClient(inputFilePath + fileName);

//            ShareFileClient toClient = targetFileShareContainerClient.getFileClient(inputFilePath + fileName);

            toClient.beginCopy(azureUtil.getFileShareFileSourceUrl(fromClient), null);
        }
    }


}
