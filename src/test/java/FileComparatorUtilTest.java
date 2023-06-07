import com.joe.java_test.utils.FileComparatorUtil;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import static org.springframework.test.util.AssertionErrors.assertTrue;


class FileComparatorUtilTest
{
	String mb32FilePath1 = "C:\\test\\get_csv\\BI_eese_salesforce_product_catalog_20220201043009.csv";
	String mb32FilePath2 = "C:\\test\\get_csv\\old\\ECOM-BI_eese_salesforce_product_catalog_20220201043009.csv";
	String mb151FilePath1 = "C:\\test\\get_csv\\0011.csv";
	String mb151FilePath2 = "C:\\test\\get_csv\\old\\0111.csv";

	@Tag("mb151group")
	@RepeatedTest(100)
	void compareMb151FilesByCRC32Test()
	{
		assertTrue("compareFilesByChecksum fail", FileComparatorUtil.compareFilesByCRC32(mb151FilePath1, mb151FilePath2));
	}

	@Tag("mb151group")
	@RepeatedTest(100)
	void compareMb151FilesByMD5Test() throws IOException, NoSuchAlgorithmException
	{
		assertTrue("compareFilesByMD5 fail", FileComparatorUtil.compareFilesByMessageDigest(mb151FilePath1, mb151FilePath2,
				FileComparatorUtil.Algorithm.MD5));
	}

	@Tag("mb151group")
	@RepeatedTest(100)
	void compareMb151FilesBySHA1Test() throws IOException, NoSuchAlgorithmException
	{
		assertTrue("compareFilesByMD5 fail", FileComparatorUtil.compareFilesByMessageDigest(mb151FilePath1, mb151FilePath2,
				FileComparatorUtil.Algorithm.SHA1));
	}

	@Tag("mb151group")
	@RepeatedTest(100)
	void compareMb151FilesBySHA256Test() throws IOException, NoSuchAlgorithmException
	{
		assertTrue("compareFilesByMD5 fail", FileComparatorUtil.compareFilesByMessageDigest(mb151FilePath1, mb151FilePath2,
				FileComparatorUtil.Algorithm.SHA256));
	}

	@Tag("mb151group")
	@RepeatedTest(100)
	void compareMb151FilesByBase64EncodingTest()
	{
		assertTrue("compareFilesByBase64Encoding fail",
				FileComparatorUtil.compareFilesByBase64Encoding(mb151FilePath1, mb151FilePath2));
	}

	@Tag("mb151group")
	@RepeatedTest(100)
	void compareMb151FilesByContentCompareTest() throws IOException
	{
		assertTrue("compareFilesByContentCompare fail",
				FileComparatorUtil.compareFilesByContentCompare(mb151FilePath1, mb151FilePath2));
	}

	@Tag("mb151group")
	@RepeatedTest(100)
	void compareMb151CSVFilesTest() throws IOException
	{
		assertTrue("compareCSVFiles fail",
				FileComparatorUtil.compareCSVFiles(mb151FilePath1, mb151FilePath2));
	}

	@Tag("mb32group")
	@RepeatedTest(100)
	void compareMb32FilesByCRC32Test()
	{
		assertTrue("compareFilesByChecksum fail", FileComparatorUtil.compareFilesByCRC32(mb32FilePath1, mb32FilePath2));
	}

	@Tag("mb32group")
	@RepeatedTest(100)
	void compareMb32FilesByMD5Test() throws IOException, NoSuchAlgorithmException
	{
		assertTrue("compareFilesByMD5 fail", FileComparatorUtil.compareFilesByMessageDigest(mb32FilePath1, mb32FilePath2,
				FileComparatorUtil.Algorithm.MD5));
	}

	@Tag("mb32group")
	@RepeatedTest(100)
	void compareMb32FilesBySHA1Test() throws IOException, NoSuchAlgorithmException
	{
		assertTrue("compareFilesByMD5 fail", FileComparatorUtil.compareFilesByMessageDigest(mb32FilePath1, mb32FilePath2,
				FileComparatorUtil.Algorithm.SHA1));
	}

	@Tag("mb32group")
	@RepeatedTest(100)
	void compareMb32FilesBySHA256Test() throws IOException, NoSuchAlgorithmException
	{
		assertTrue("compareFilesByMD5 fail", FileComparatorUtil.compareFilesByMessageDigest(mb32FilePath1, mb32FilePath2,
				FileComparatorUtil.Algorithm.SHA256));
	}

	@Tag("mb32group")
	@RepeatedTest(100)
	void compareMb32FilesByBase64EncodingTest()
	{
		assertTrue("compareFilesByBase64Encoding fail",
				FileComparatorUtil.compareFilesByBase64Encoding(mb32FilePath1, mb32FilePath2));
	}

	@Tag("mb32group")
	@RepeatedTest(100)
	void compareMb32FilesByContentCompareTest() throws IOException
	{
		assertTrue("compareFilesByContentCompare fail",
				FileComparatorUtil.compareFilesByContentCompare(mb32FilePath1, mb32FilePath2));
	}

	@Tag("mb151group")
	@RepeatedTest(100)
	void compareMb32CSVFilesTest() throws IOException
	{
		assertTrue("compareCSVFiles fail",
				FileComparatorUtil.compareCSVFiles(mb32FilePath1, mb32FilePath2));
	}
}
