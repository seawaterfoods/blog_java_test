package com.joe.java_test.utils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import com.ibm.icu.text.CharsetDetector;
import com.shoalter.ecommerce.infra.reportserver.enums.Algorithm;
import org.apache.commons.io.input.BOMInputStream;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.zip.CRC32;


public class FileComparatorUtil
{
	private static final String BOM = "-BOM";

	public enum LineSeparator {
		WINDOWS("\r\n"),
		LINUX("\n"),
		MAC("\r"),
		UNKNOWN("");
		private final String code;

		LineSeparator(String code) {
			this.code = code;
		}

		public String getCode() {
			return code;
		}
	}
	public enum Algorithm
	{
		MD5("MD5"),
		SHA1("SHA-1"),
		SHA256("SHA-256");
		private final String code;

		Algorithm(String code)
		{
			this.code = code;
		}

		public String getCode()
		{
			return code;
		}
	}

	private FileComparatorUtil()
	{
		throw new IllegalStateException("Utility class");
	}

	public static boolean compareFilesByCRC32(String filePath1, String filePath2) throws IOException {
		syncEncodingAndLineSeparator(filePath1, filePath2);
		CRC32 crc1 = calculateChecksumForCRC32(filePath1);
		CRC32 crc2 = calculateChecksumForCRC32(filePath2);

		return crc1.getValue() == crc2.getValue();
	}

	public static boolean compareFilesByMessageDigest(String filePath1, String filePath2,
			Algorithm algorithmCode)
			throws IOException, NoSuchAlgorithmException {
		syncEncodingAndLineSeparator(filePath1, filePath2);
		String md51 = calculateChecksumForAlgorithm(filePath1, algorithmCode);
		String md52 = calculateChecksumForAlgorithm(filePath2, algorithmCode);

		return md51.equals(md52);
	}

	private static void syncEncodingAndLineSeparator(String filePath1, String filePath2)
			throws IOException {
		String filePath1Encoding = getFilesEncoding(filePath1);
		String filePath2Encoding = getFilesEncoding(filePath2);
		String filePath2LineSeparator = getFilesLineSeparator(filePath2, filePath2Encoding);
		File file1 = new File(filePath1);
		File file2 = new File(filePath2);
		String fileContent1 = inputFileRead(file1, filePath1Encoding, filePath2LineSeparator);
		outputFileWrite(file1, fileContent1, filePath2Encoding);
		String fileContent2 = inputFileRead(file2, filePath2Encoding, filePath2LineSeparator);
		outputFileWrite(file2, fileContent2, filePath2Encoding);

	}

	private static String getFilesEncoding(String filePath) throws IOException {
		CharsetDetector filePathDetector = new CharsetDetector();
		StringBuilder filesEncoding = new StringBuilder();
		try (FileInputStream fileInputStream = new FileInputStream(filePath)) {
			filePathDetector.setText(new BufferedInputStream(fileInputStream));
			filesEncoding.append(filePathDetector.detect().getName());
			if (StandardCharsets.UTF_8.name().contentEquals(filesEncoding) && (BOMInputStream.builder()
					.setInputStream(new FileInputStream(filePath)).get().hasBOM())) {
				filesEncoding.append(BOM);
			}
		}
		return filesEncoding.toString();
	}

	private static String getFilesLineSeparator(String filePath, String encode) throws IOException {
		byte[] pathResourceFileBytes = Files.readAllBytes(Paths.get(filePath));
		String filePathFileContent = new String(pathResourceFileBytes, encode.replace(BOM, ""));
		if (filePathFileContent.contains(LineSeparator.WINDOWS.getCode())) {
			return LineSeparator.WINDOWS.getCode();
		} else if (filePathFileContent.contains(LineSeparator.MAC.getCode())) {
			return LineSeparator.MAC.getCode();
		} else if (filePathFileContent.contains(LineSeparator.LINUX.getCode())) {
			return LineSeparator.LINUX.getCode();
		}
		return LineSeparator.UNKNOWN.getCode();
	}

	private static String inputFileRead(File file, String filePath1Encoding, String lineSeparator)
			throws IOException {
		StringBuilder fileContent = new StringBuilder();

		try (InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(file),
				filePath1Encoding.replace(BOM, ""));
				BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				fileContent.append(line);
				fileContent.append(lineSeparator);
			}
		}
		return fileContent.substring(0, fileContent.length() - lineSeparator.length());
	}

	private static void outputFileWrite(File file, String fileContent, String filePath2Encoding)
			throws IOException {
		try (OutputStreamWriter outputStreamWriter = new OutputStreamWriter(new FileOutputStream(file),
				filePath2Encoding.replace(BOM, ""));
				BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter)) {
			bufferedWriter.write(fileContent.replace("\ufeff", ""));
		}
	}

	public static boolean compareFilesByBase64Encoding(String filePath1, String filePath2)
	{
		String encodedContent1 = calculateBase64Encoding(filePath1);
		String encodedContent2 = calculateBase64Encoding(filePath2);

		return encodedContent1.equals(encodedContent2);
	}

	public static boolean compareFilesByContentCompare(String filePath1, String filePath2) throws IOException
	{
		Path path1 = Paths.get(filePath1);
		Path path2 = Paths.get(filePath2);

		byte[] fileBytes1 = Files.readAllBytes(path1);
		byte[] fileBytes2 = Files.readAllBytes(path2);

		return compareByteArrays(fileBytes1, fileBytes2);
	}

	public static boolean compareCSVFiles(String file1Path, String file2Path) throws IOException {
		CSVFormat csvFormat = CSVFormat.DEFAULT;
		try (CSVParser csvParser1 = new CSVParser(new FileReader(file1Path), csvFormat);
				CSVParser csvParser2 = new CSVParser(new FileReader(file2Path), csvFormat)) {
			for (CSVRecord record1 : csvParser1) {
				CSVRecord record2 = csvParser2.iterator().next();
				if (record1.size() != record2.size()) {
					return false;
				}
				for (int i = 0; i < record1.size(); i++) {
					String value1 = record1.get(i);
					String value2 = record2.get(i);

					if (!value1.equals(value2)) {
						return false;
					}
				}
			}

			if (csvParser2.iterator().hasNext()) {
				return false;
			}
		}

		return true;
	}

	private static CRC32 calculateChecksumForCRC32(String filePath)
	{
		CRC32 crc = new CRC32();
		try (FileInputStream fis = new FileInputStream(filePath))
		{
			byte[] buffer = new byte[8192];
			int bytesRead;
			while ((bytesRead = fis.read(buffer)) != -1)
			{
				crc.update(buffer, 0, bytesRead);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return crc;
	}

	private static String calculateChecksumForAlgorithm(String filePath, Algorithm algorithmCode)
			throws IOException, NoSuchAlgorithmException
	{
		byte[] data = Files.readAllBytes(Paths.get(filePath));
		byte[] hash = MessageDigest.getInstance(algorithmCode.getCode()).digest(data);
		return new BigInteger(1, hash).toString(16);
	}

	private static String calculateBase64Encoding(String filePath)
	{
		File file = new File(filePath);
		byte[] fileBytes = new byte[(int) file.length()];

		try (FileInputStream fis = new FileInputStream(filePath))
		{
			DataInputStream dis = new DataInputStream(fis);
			dis.readFully(fileBytes);
			return new String(fileBytes, StandardCharsets.UTF_8);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return Base64.getEncoder().encodeToString(fileBytes);
	}

	private static boolean compareByteArrays(byte[] bytes1, byte[] bytes2)
	{
		if (bytes1.length != bytes2.length)
		{
			return false;
		}

		for (int i = 0; i < bytes1.length; i++)
		{
			if (bytes1[i] != bytes2[i])
			{
				return false;
			}
		}

		return true;
	}
}