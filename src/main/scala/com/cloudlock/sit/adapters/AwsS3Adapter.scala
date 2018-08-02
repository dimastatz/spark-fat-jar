package com.dimastatz.sit.adapters

import java.io._
import java.util.zip._
import java.util.Date
import java.util.UUID.randomUUID
import java.text.SimpleDateFormat

import com.amazonaws.AmazonClientException
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ObjectMetadata

import scala.util.{Failure, Success, Try}


object AwsS3Adapter extends Serializable{
  def save(path: String, data: String, tenant: String, device: String): String = {
    val normalizedPath = normalizeS3Path(path)
    val credentials = getCredentials(normalizedPath)

    val client = if(credentials.isEmpty) new
        AmazonS3Client() else new AmazonS3Client(credentials.get)

    val parts = normalizedPath.split("/")
    val bucketName = if(parts(0).contains("@")) parts(0).split("@").last else parts(0)
    val key = if(parts.length == 1) None else Some(parts.drop(1).mkString("/"))

    val fileName = createFileName(tenant, device, key)

    val compressed = compress(data)
    val metadata = new ObjectMetadata()
    metadata.setContentLength(compressed.available())
    val result = client.putObject(bucketName, fileName, compressed, metadata)
    result.getContentMd5
  }

  def compress(content: String): ByteArrayInputStream = {
    val arrOutputStream = new ByteArrayOutputStream()
    val zipOutputStream = new GZIPOutputStream(arrOutputStream)
    zipOutputStream.write(content.getBytes)
    zipOutputStream.flush()
    zipOutputStream.close()
    new ByteArrayInputStream(arrOutputStream.toByteArray)
  }

  def normalizeS3Path(path: String): String = {
    if(path.toLowerCase.startsWith("s3"))
      path.split("//")(1)
    else path
  }

  def getCredentials(path: String): Option[BasicAWSCredentials] = {
    val credentials = path.split("@").head.split(":")
    if (credentials.length == 1) None
    else Some(new BasicAWSCredentials(credentials(0), credentials(1)))
  }

  def createFileName(tenant: String, device: String, prefix: Option[String]): String = {
    val fileUUID = randomUUID().toString
    val timeStamp = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    val longTimeStamp = new SimpleDateFormat("yyyy-MM-dd'T'HH.mm").format(new Date())

    if(prefix.isEmpty) s"$tenant/$device/norm/$timeStamp/ls.s3.$fileUUID.$longTimeStamp.part0.txt.gz"
    else s"${prefix.get}/$tenant/$device/norm/$timeStamp/ls.s3.$fileUUID.$longTimeStamp.part0.txt.gz"
  }

  def isProd2Mode(bucketPath: String): Boolean = {
    val fullPath = bucketPath.substring(0, bucketPath
      .lastIndexOf('/') + 1) + "originid_temp_feature_file_flag.txt"

    val normalizedPath = normalizeS3Path(fullPath)
    val credentials = getCredentials(normalizedPath)
    val client = if(credentials.isEmpty) new
        AmazonS3Client() else new AmazonS3Client(credentials.get)

    val bucketName = normalizedPath.substring(0, normalizedPath.indexOf('/'))
    val key = normalizedPath.substring(normalizedPath.indexOf('/') + 1)

    Try(client.getObjectMetadata(bucketName, key)).isSuccess
  }
}
