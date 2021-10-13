import com.google.api.gax.paging.Page;
import com.google.cloud.storage.*;
import com.joe.blog_java_test.utils.GoogleCloudStorageUtil;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

public class googleClouldStorageTest
{

    GoogleCloudStorageUtil gcsUtil;

    public googleClouldStorageTest() throws IOException
    {
    }

    @Test
    public void showGCSBucketTest() throws IOException
    {
        gcsUtil = new GoogleCloudStorageUtil("C:\\work\\hktv_bfes\\src\\main\\resources\\google\\key\\svc_acc_for_gcp_bq.json");
        // The ID of your GCP project
        Page<Bucket> buckets = gcsUtil.getStorage().list();

        for (Bucket bucket : buckets.iterateAll())
        {
            System.out.println(bucket.getName());
        }
    }

    @Test
    public void createGCSBucketTest() throws IOException
    {
        gcsUtil = new GoogleCloudStorageUtil("C:\\work\\hktv_bfes\\src\\main\\resources\\google\\key\\svc_acc_for_gcp_bq.json");
        String createBucketName = "eese_mall_dev_bucket";
        // The ID of your GCP project
        // See the StorageClass documentation for other valid storage classes:
        // https://googleapis.dev/java/google-cloud-clients/latest/com/google/cloud/storage/StorageClass.html
        StorageClass storageClass = StorageClass.STANDARD;

        // See this documentation for other valid locations:
        // http://g.co/cloud/storage/docs/bucket-locations#location-mr
        String location = "ASIA-EAST2";

        Bucket bucket =
                gcsUtil.getStorage().create(
                        BucketInfo.newBuilder(createBucketName)
                                .setStorageClass(storageClass)
                                .setLocation(location)
                                .build());

        System.out.println(
                "Created bucket "
                        + bucket.getName()
                        + " in "
                        + bucket.getLocation()
                        + " with storage class "
                        + bucket.getStorageClass());

        Page<Bucket> buckets = gcsUtil.getStorage().list();

        for (Bucket bucket1 : buckets.iterateAll())
        {
            System.out.println(bucket1.getName());
        }
    }

    @Test
    public void deleteGCSBucketTest() throws IOException
    {
        gcsUtil = new GoogleCloudStorageUtil("C:\\work\\hktv_bfes\\src\\main\\resources\\google\\key\\svc_acc_for_gcp_bq.json");
        String deleteBucketName = "eese_mall_dev_bucket";
        // The ID of your GCP project
        // String projectId = "your-project-id";

        // The ID of the bucket to delete
        // String bucketName = "your-unique-bucket-name";

        Storage storage = gcsUtil.getStorage();
        Bucket bucket = storage.get(deleteBucketName);
        bucket.delete();

        System.out.println("Bucket " + bucket.getName() + " was deleted");

        Page<Bucket> buckets = gcsUtil.getStorage().list();

        for (Bucket bucket1 : buckets.iterateAll())
        {
            System.out.println(bucket1.getName());
        }
    }

    @Test
    public void GoogleCloudStorageLinkTest() throws IOException
    {
        gcsUtil = new GoogleCloudStorageUtil("C:\\work\\hktv_bfes\\src\\main\\resources\\google\\key\\svc_acc_for_gcp_bq.json");
        String bucketName = "test_create_bucket_tony";
        String storageFileName = "testFolder";
        BlobId blobId = BlobId.of(bucketName, storageFileName + "/");
        Blob blob =
                gcsUtil.getStorage().get(blobId);


        // Print blob metadata
        System.out.println("Bucket: " + blob.getBucket());
        System.out.println("blob.getName(): " + blob.getName());
        System.out.println("CacheControl: " + blob.getCacheControl());
        System.out.println("ComponentCount: " + blob.getComponentCount());
        System.out.println("ContentDisposition: " + blob.getContentDisposition());
        System.out.println("ContentEncoding: " + blob.getContentEncoding());
        System.out.println("ContentLanguage: " + blob.getContentLanguage());
        System.out.println("ContentType: " + blob.getContentType());
        System.out.println("CustomTime: " + blob.getCustomTime());
        System.out.println("Crc32c: " + blob.getCrc32c());
        System.out.println("Crc32cHexString: " + blob.getCrc32cToHexString());
        System.out.println("ETag: " + blob.getEtag());
        System.out.println("Generation: " + blob.getGeneration());
        System.out.println("Id: " + blob.getBlobId());
        System.out.println("KmsKeyName: " + blob.getKmsKeyName());
        System.out.println("Md5Hash: " + blob.getMd5());
        System.out.println("Md5HexString: " + blob.getMd5ToHexString());
        System.out.println("MediaLink: " + blob.getMediaLink());
        System.out.println("Metageneration: " + blob.getMetageneration());
        System.out.println("Name: " + blob.getName());
        System.out.println("Size: " + blob.getSize());
        System.out.println("StorageClass: " + blob.getStorageClass());
        System.out.println("TimeCreated: " + new Date(blob.getCreateTime()));
        System.out.println("Last Metadata Update: " + new Date(blob.getUpdateTime()));
        Boolean temporaryHoldIsEnabled = (blob.getTemporaryHold() != null && blob.getTemporaryHold());
        System.out.println("temporaryHold: " + (temporaryHoldIsEnabled ? "enabled" : "disabled"));
        Boolean eventBasedHoldIsEnabled =
                (blob.getEventBasedHold() != null && blob.getEventBasedHold());
        System.out.println("eventBasedHold: " + (eventBasedHoldIsEnabled ? "enabled" : "disabled"));

    }

    @Test
    public void getGCSFilesTest() throws IOException
    {
        gcsUtil = new GoogleCloudStorageUtil("C:\\work\\hktv_bfes\\src\\main\\resources\\google\\key\\svc_acc_for_gcp_bq.json");
        String bucketName = "eese_mall_dev_bucket";
//        如果要取bucket中所有對象，可以使用此方法。
        Page<Blob> blobs1 = gcsUtil.getStorage().list(bucketName);

        String outputFiles = "testFolder/output";
        String localOutputPath = "C:\\test\\bfes\\tmp\\output";
        String inputFiles = "testFolder/input";
        String localInputPath = "C:\\test\\bfes\\tmp\\input";

        String directoryPrefix = "testFolder/input";
//        在GCS中File是以Blob方式敘述名稱，folder有'/'結尾。
//        如果要取特定資料夾可以用以下方法取得。
        Page<Blob> blobs2 =
                gcsUtil.getStorage().list(
                        bucketName,
                        Storage.BlobListOption.prefix("(secondFolderName)/"));

//        blobs1是全部 blobs2是特定folder
        for (Blob blob : blobs1.iterateAll())
        {
            if (blob.getName().equals("/"))
                continue;
            System.out.println("-------------");
            System.out.println("Bucket: " + blob.getBucket());
            System.out.println("blob.getName(): " + blob.getName());
        }

    }

    @Test
    public void uploadGCSObjectTest() throws IOException
    {
        gcsUtil = new GoogleCloudStorageUtil("C:\\work\\hktv_bfes\\src\\main\\resources\\google\\key\\svc_acc_for_gcp_bq.json");
        String bucketName = "test_create_bucket_tony";
        Page<Blob> blobs1 = gcsUtil.getStorage().list(bucketName);

//        String outputFiles = "testFolder/output";
//        String localOutputPath = "C:\\test\\bfes\\tmp\\output";
        String inputFiles = "testFolder/input";
        String localInputPath = "C:\\test\\bfes\\tmp\\input";
//        Input
        File[] fileArray = new File(localInputPath).listFiles();
//        Output
//        File[] fileArray = new File(localOutputPath).listFiles();

        for (File currentFile : fileArray)
        {
            if (!currentFile.isDirectory())
            {
                System.out.println("--------------");
//                確認檔案名稱後拼接一起
//                Input
                System.out.println("currentFile.getName():" + currentFile.getName());
                String localInputObject = localInputPath + "/" + currentFile.getName();
                System.out.println("localInputObject:" + localInputObject);
                String GCSInputObject = inputFiles + "/" + currentFile.getName();
                System.out.println("GCSInputObject:" + GCSInputObject);
//                Output
//                System.out.println("currentFile.getName():"+currentFile.getName());
//                String localOutputObject = localOutputPath+"/"+currentFile.getName();
//                System.out.println("localOutputObject:"+localOutputObject);
//                String GCSOutputObject = outputFiles+"/"+currentFile.getName();
//                System.out.println("GCSOutputObject:"+GCSOutputObject);


//        上傳檔案時，若無資料夾GCS會自動create該路徑上所有Folder。
                Storage storage = gcsUtil.getStorage();
//                Input
                BlobId inBlobId = BlobId.of(bucketName, GCSInputObject);
                BlobInfo blobInfo = BlobInfo.newBuilder(inBlobId).build();
                storage.create(blobInfo, Files.readAllBytes(Paths.get(localInputObject)));
                System.out.println(
                        "File " + localInputObject + " uploaded to bucket " + bucketName + " as " + GCSInputObject);
            }
//                Output
//                BlobId outBlobId = BlobId.of(bucketName, GCSOutputObject);
//                BlobInfo blobInfo = BlobInfo.newBuilder(outBlobId).build();
//                storage.create(blobInfo, Files.readAllBytes(Paths.get(localOutputObject)));
//                System.out.println(
//                        "File " + localOutputObject + " uploaded to bucket " + bucketName + " as " + GCSOutputObject);
//            }
        }

//        如果上傳有同名的檔案，會直接覆蓋上去，不會報錯。


        Page<Blob> blobs2 =
                gcsUtil.getStorage().list(
                        bucketName,
                        Storage.BlobListOption.prefix(inputFiles),
                        Storage.BlobListOption.currentDirectory());
        for (Blob blob : blobs2.iterateAll())
        {
            String objectName = blob.getName().replace(inputFiles, "");
            if (objectName.equals(""))
                continue;
            System.out.println("-------------");
            System.out.println("Bucket: " + blob.getBucket());
            System.out.println("blob.getName(): " + blob.getName());
        }
    }


    @Test
    public void downlandGCSFilesTest() throws IOException
    {
        gcsUtil = new GoogleCloudStorageUtil("C:\\work\\hktv_bfes\\src\\main\\resources\\google\\key\\svc_acc_for_gcp_bq.json");
        String bucketName = "eese_mall_dev_bucket";
        Page<Blob> blobs1 = gcsUtil.getStorage().list(bucketName);

        String outputFiles = "testFolder/output";
        String localOutputPath = "C:\\test\\bfes\\postoffice\\google";
        String inputFiles = "testFolder/input";
        String localInputPath = "C:\\test\\bfes\\tmp\\input";
        Storage storage = gcsUtil.getStorage();

        Page<Blob> blobs2 =
                gcsUtil.getStorage().list(
                        bucketName,
                        Storage.BlobListOption.prefix( "(secondFolderName)/"));
        for (Blob blob : blobs2.iterateAll())
        {
            System.out.println(blob.getName());
            String objectName = blob.getName().split("/")[2];
            if (objectName.equals("/") )
                continue;
            System.out.println("-------------");
            System.out.println("filesName:" + objectName);
            System.out.println("Bucket: " + blob.getBucket());
            System.out.println("blob.getName(): " + blob.getName());
            String localOutputObject = localOutputPath+"\\" + objectName;

//            blob.downloadTo(Paths.get(localOutputObject));
            System.out.println(
                    "Downloaded object "
                            + objectName
                            + " from bucket name "
                            + bucketName
                            + " to "
                            + localOutputObject);
        }
    }

    @Test
    public void moveGCSFilesTest() throws Exception
    {
        gcsUtil = new GoogleCloudStorageUtil("C:\\work\\hktv_bfes\\src\\main\\resources\\google\\key\\HA.json");
        String bucketName = "joewutest1";
        Page<Blob> blobs1 = gcsUtil.getStorage().list(bucketName);

        String outputFiles = "output/";
        String localOutputPath = "C:\\test\\bfes\\tmp\\output\\";
        String inputFiles = "input/";
        String localInputPath = "C:\\test\\bfes\\tmp\\input\\";

        final long _EXPIRED_DATE = 7;
        final String _BACKUP_FOLDER = "bfes-transferred/";

        Storage storage = gcsUtil.getStorage();

        Page<Blob> blobs2 =
                gcsUtil.getStorage().list(
                        bucketName,
                        Storage.BlobListOption.prefix(outputFiles),
                        Storage.BlobListOption.currentDirectory());
        for (Blob blob : blobs1.iterateAll())
        {
            System.out.println("blob.getName(): " + blob.getName());
            String objectName = blob.getName().replace(outputFiles, "");
            if (!blob.getName().contains(outputFiles) || blob.getName().contains(_BACKUP_FOLDER) || objectName.equals(""))
                continue;
            System.out.println("-------------");
            System.out.println("filesName:" + objectName);
            System.out.println("Bucket: " + blob.getBucket());
            System.out.println("blob.getName(): " + blob.getName());

//            // Write a copy of the object to the target bucket
//            CopyWriter copyWriter = blob.copyTo(bucketName, outputFiles + _BACKUP_FOLDER + objectName);
//            Blob copiedBlob = copyWriter.getResult();
////            直接刪除!!
//            storage.delete(bucketName, blob.getName());
//
//            System.out.println("Object " + blob.getName() + " was move from bucket:" + bucketName + " to " + bucketName + " object:" + copiedBlob.getName());
            System.out.println();
        }
        checkBackupExpiredFile(bucketName,outputFiles);
    }

    public void testJsonStringConnGCSUtil() throws IOException
    {
        String json = "{   \"type\": \"service_account\",   \"project_id\": \"eese-test-2021\",   \"private_key_id\": \"be66f83d8084c34a9bf731937d5f2f3e713dcf94\",   \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCqn2XTAIcVRoOt\\nC2pRD2gJGgHaB9JfvTXxNjLkzB7oQ+yKy/hEt9SbiWAQ7aQPa6JDTDXq7M5rhTo7\\nF9yLusWOaQophmgqZgQTQRnIsQL2ieBFFzYjl9B24D/5xzvA7A+/D7fp3C77lmFN\\nXmpTQDuYwUhchjcX4sdPm75hxz95c6xTqfEixyQJFwsLf5PjbW1xHABgLfqYrEek\\nH+CvpW9iyxIRoRImR5NZPQdG4Be0yKS2P1u84RUTgXf/lKMQMvUJvEn4tcmipo6d\\nomvwVlcRvcZhxrijDGRYmBdRuCT646BPmM0UfWACkdUPGyaumTGa8PRzt4dHYb7N\\nqhK9n6NvAgMBAAECggEAAgzbSN9LQD6yK1ylQOcZ/W4CErAgX4UjBkx77RLk1i6m\\nw34y9ARvehT7AgkQ/RxyfTOHvFRvIIyfyAD/5e/vGsh5VklmrAfAychw+l/iJLtA\\nZiQiwue6mgsUybgSqD1N7SJPt/As4loKvkbBqVFuxeWgrHfyZoeTlInPDla6FQgB\\nRahv1g3O+K8vvQk/w+eXMRC4GpHin9GYk8Sk8BtIaJyJc9NrinaSCL/mDq3ZNSWD\\n/8n0/RUH40OaR9ny/y0wYPkfbVwX3/wbg7Ez0akzN9UFyezyUGVzTFUdLPb8MFH8\\nBr/9Y3u7OPRjxuh6f6jP0YOZ/RgWlsOwjMW63RctpQKBgQDZ7zoHl+AV3NXmFZfr\\nNJK/urzV/OULuDbWWG6r1BZ2BvoEtPPQfHY5TRdWTnKIYF3taACXunMzFg/89Ljf\\nOqhzr6/iF/mekXffs1Ks0zVBPRGeemZBm/zhWMa6OFosp1GqI0IHCmGACeYXqpA6\\ntoDOZWuMCRpLq+oXtAikp1IjzQKBgQDIbKij+oUyvyOWV0UvDall0bUYFuUSFOMw\\nz8PU4LAgkKjiVWdoYsByz93pWK6c0nmfUDW8W36iV1SyUiN+jSaGgBUNgvFnq0aT\\nrHnXNs3nfxkuQnwubGY+pALvhZD1I5CMcjnL5k+ofCEEppz+dCy2Us3mUjVPfCtR\\nx3UDMcIgKwKBgGs59g5IXnvDRQbItw+FtXJaOZD7teI3R9vcdM9cMZTux9LLCW8Z\\n3b1Bveq7/EQ3Td+CHVCJ7yZCg6lM6y0CGmGZHmuaqjmnjxhkjZf07y3jEykrReag\\nDiL94AE9urhftqGEaXc3V1N9C1W3mPEaXHOwmOK0k9iJhEsXTG1e2d49AoGABAZK\\njKBnNFGqQUaXWQ6JLEY9wLIU/3vz4/MIF6o7XywIeCMG5I23F5cc+aaSOwvu7UgL\\nDDE6JcBjLwF/PPdQorrHsXHDrzU9QZHhEHVFRTTEKK2lGLmMhA1/EZg5BPMl3+wb\\nRQDm8Jctgo5l8KYcj7yx7cIQ1/vCUu4SZ3IEV+kCgYBokf7FI2QFbF999S+SsluE\\ndCSrp1w7H4xszSUUrP9uIJuCZULRawLGMXN6nmTuOPIZonSMG6+D5OEXZQHAM3ZQ\\njXkeKTW5KdoUkAcYEblike8bKiyUzo1CuSB29brwGZu57e84wAMlYv8CR+CrIk3k\\nrZ6KjOcN5tUWmfGn6QFsaw==\\n-----END PRIVATE KEY-----\\n\",   \"client_email\": \"eese-test-2021@eese-test-2021.iam.gserviceaccount.com\",   \"client_id\": \"109554285288860187049\",   \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",   \"token_uri\": \"https://oauth2.googleapis.com/token\",   \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",   \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/eese-test-2021%40eese-test-2021.iam.gserviceaccount.com\" }";

        InputStream credentialsStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));

        String bucketName = "eese_mall_dev_bucket";
//        如果要取bucket中所有對象，可以使用此方法。
        Page<Blob> blobs = gcsUtil.getStorage().list(bucketName);

        for (Blob blob : blobs.iterateAll())
        {
            System.out.println("-------------");
            System.out.println("Bucket: " + blob.getBucket());
            System.out.println("blob.getName(): " + blob.getName());
        }
    }

    @Test
    public void deleteGCSFilesTest() throws IOException
    {

        gcsUtil = new GoogleCloudStorageUtil("C:\\work\\hktv_bfes\\src\\main\\resources\\google\\key\\svc_acc_for_gcp_bq.json");
        String bucketName = "eese_mall_dev_bucket";
        Page<Blob> blobs1 = gcsUtil.getStorage().list(bucketName);

        String outputFiles = "testFolder/output/";
        String localOutputPath = "C:\\test\\bfes\\tmp\\output\\";
        String inputFiles = "(secondFolderName)/";
        String localInputPath = "C:\\test\\bfes\\tmp\\input\\";
        Storage storage = gcsUtil.getStorage();

        Page<Blob> blobs2 =
                gcsUtil.getStorage().list(
                        bucketName,
                        Storage.BlobListOption.prefix(inputFiles));
        for (Blob blob : blobs2.iterateAll())
        {

            String objectName = blob.getName().split("/")[2];
            if (objectName.equals(""))
                continue;
            System.out.println("-------------");
            System.out.println("filesName:" + objectName);
            System.out.println("Bucket: " + blob.getBucket());
            System.out.println("blob.getName(): " + blob.getName());

//            直接刪除!!
//            storage.delete(bucketName, blob.getName());

            System.out.println("Object " + blob.getName() + " was deleted from " + bucketName);
            System.out.println();
        }
    }

    private void checkBackupExpiredFile(String bucketName,String filePath) throws Exception
    {
        gcsUtil = new GoogleCloudStorageUtil("C:\\work\\hktv_bfes\\src\\main\\resources\\google\\key\\HA.json");

        final long _EXPIRED_DATE = 7;
        final String _BACKUP_FOLDER = "bfes-transferred/";

        Storage storage = gcsUtil.getStorage();

        String backupPath = filePath + _BACKUP_FOLDER;
        Page<Blob> blobs =
                gcsUtil.getStorage().list(
                        bucketName,
                        Storage.BlobListOption.prefix(backupPath),
                        Storage.BlobListOption.currentDirectory());
        for (Blob blob : blobs.iterateAll())
        {
            System.out.println("blob.getName(): " + blob.getName());
            String objectName = blob.getName().replace(backupPath, "");
            if (objectName.equals(""))
                continue;

            LocalDate dataDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(blob.getCreateTime()), ZoneId.systemDefault()).toLocalDate();
            System.out.println(dataDate);
            System.out.println(LocalDate.now().toEpochDay() - dataDate.toEpochDay());

            if (LocalDate.now().toEpochDay() - dataDate.toEpochDay() > _EXPIRED_DATE)
            {
//                log.info("FileShare System:[{}] Delete Expired File:[{}]", sourceClient.getBlobName(), blobItem.getName());
                storage.delete(bucketName, blob.getName());
            }

        }
    }
}
