import com.joe.blog_java_test.utils.AzureUtil;
import com.joe.blog_java_test.utils.LocalFileUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.regex.Pattern;


public class AzureTest
{

    String connectionStr = "AccountName=postofficeforlocal;AccountKey=QfBLkyzTtw5BbermMLiRNdh4Kd4ByujtRZM2mJlS7+1uObdDuv4b5obctyii27AuXa85reAeUXEBOsHtk0MJOw==;EndpointSuffix=core.windows.net;DefaultEndpointsProtocol=https";

    String bfesLocalAzureOutputFilePath = "C:" + File.separator + "test" + File.separator + "bfes" + File.separator + "postoffice" + File.separator + "azure" + File.separator + "output" + File.separator;
    String bfesLocalAzureInputFilePath = "C:" + File.separator + "test" + File.separator + "bfes" + File.separator + "postoffice" + File.separator + "azure" + File.separator + "input" + File.separator;
    String regex = "^[0-9a-zA-Z]{1,}-[0-9a-zA-Z]*_.*";
    String outputFilePath = "file_import" + File.separator + "output" + File.separator;
    String inputFilePath = "file_import" + File.separator + "input" + File.separator;
    String _TAIPEI = "taipei";
    String _KAOHSIUNG = "kaohsiung";

    @Test
    public void downloadAzureBlobFilesTest() throws Exception
    {
        AzureUtil azureUtil = new AzureUtil(connectionStr);

        Assertions.assertTrue(azureUtil.batchDownloadBlobFilesToLocal(_TAIPEI, outputFilePath, bfesLocalAzureOutputFilePath, regex));
    }

    @Test
    public void uploadAzureBlobFilesTest() throws Exception
    {
        AzureUtil azureUtil = new AzureUtil(connectionStr);

        Assertions.assertTrue(azureUtil.batchUploadLocalFilesToBlob(_TAIPEI, inputFilePath, bfesLocalAzureInputFilePath, regex));
    }

    @Test
    public void localTest() throws Exception
    {
        LocalFileUtil.checkFolderExist(bfesLocalAzureInputFilePath);
        Pattern pattern = Pattern.compile(regex);
        File fileDir = new File(bfesLocalAzureInputFilePath);
        System.out.println("bfesLocalAzureInputFilePath:"+bfesLocalAzureInputFilePath);
        System.out.println("fileDir.getAbsolutePath():"+fileDir.getAbsolutePath());
        Arrays.stream(fileDir.list()).forEach(System.out::println);
    }


}
