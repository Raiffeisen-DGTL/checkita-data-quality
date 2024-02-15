package ru.raiffeisen.checkita.config

import com.typesafe.config._
import eu.timepit.refined.api.RefType
import ru.raiffeisen.checkita.config.RefinedTypes.{EncryptionKey, SparkParam}
import ru.raiffeisen.checkita.utils.ResultUtils.{Result, TryOps}

import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.util.Base64
import javax.crypto.spec.{IvParameterSpec, PBEKeySpec, SecretKeySpec}
import javax.crypto.{Cipher, SecretKeyFactory}
import scala.collection.JavaConverters._
import scala.util.Try


/**
 * Class that holds methods to encrypt and decrypt certain fields in configuration.
 * This functionality is required to safely store configuration with sensitive data
 * in the Data Quality storage.
 *
 * @param secret Secret string used to encrypt/decrypt sensitive fields
 * @param keyFields List of key fields used to identify fields that requires encryption/decryption.
 *                  If field name contains some of these fields then it will be subjected to encryption/decryption.
 */
class ConfigEncryptor(secret: EncryptionKey, keyFields: Seq[String] = Seq("password", "secret")) {

  private val algorithm = "AES"
  private val cipher = "AES/CBC/PKCS5Padding"
  private val keyLength = 256
  private val keyGenCount = 65536
  private val keyGenAlgorithm = "PBKDF2WithHmacSHA256"
  private val ivLength = 16
  private val saltTail = "Checkita"

  /**
   * Function that checks whether field value should be encrypted based on field name
   */
  private val shouldEncrypt = (key: String) => keyFields.map(_.toLowerCase).exists(key.toLowerCase.contains)

  /**
   * Generates AES256 secret key used later in Cipher to encrypt/decrypt string values.
   *
   * @param key User defined secret key
   * @return AES256 Secret key
   */
  private def getSecretKeySpec(key: EncryptionKey): SecretKeySpec = {
    val keyChars = key.value.toCharArray
    // use digits from password as salt (we need deterministic salt here thought it would weaken encryption)
    // In addition append some tail to ensure that salt is never empty:
    val salt = (keyChars.filter(_.isDigit) ++ saltTail.toCharArray).map(_.toByte)

    val factory = SecretKeyFactory.getInstance(keyGenAlgorithm)
    val spec = new PBEKeySpec(keyChars, salt, keyGenCount, keyLength)
    new SecretKeySpec(factory.generateSecret(spec).getEncoded, algorithm)
  }

  /**
   * Generates random initialization vector (IV) for use during encryption.
   *
   * @return Random IV
   */
  private def generateIV: (Array[Byte], IvParameterSpec) = {
    val iv = new Array[Byte](ivLength)
    new SecureRandom().nextBytes(iv)
    iv -> new IvParameterSpec(iv)
  }

  /**
   * Retrieves initialization vector (IV) from encrypted array of bytes for use during decryption.
   *
   * @param encrypted Encrypted array of bytes
   * @return IV used to encrypt provided data
   */
  private def extractIV(encrypted: Array[Byte]): (Array[Byte], IvParameterSpec) = {
    val iv = new Array[Byte](ivLength)
    Array.copy(encrypted, 0, iv, 0, ivLength)
    iv -> new IvParameterSpec(iv)
  }

  /**
   * Encrypts string value using AES256 algorithm. Uses user-defined secret key for encryption.
   *
   * @param value String value to encrypt
   * @param key   User-defined secret key
   * @return Either encrypted string or a list of encryption errors.
   */
  private def encrypt(value: String, key: EncryptionKey): String = {
    val aesKey = getSecretKeySpec(key)
    val (iv, ivSpec) = generateIV

    val c = Cipher.getInstance(cipher)
    c.init(Cipher.ENCRYPT_MODE, aesKey, ivSpec)
    val cipherBytes = c.doFinal(value.getBytes(StandardCharsets.UTF_8))
    val encrypted = new Array[Byte](ivLength + cipherBytes.length)

    // Prepend IV to cipher output:
    Array.copy(iv, 0, encrypted, 0, ivLength)
    Array.copy(cipherBytes, 0, encrypted, ivLength, cipherBytes.length)

    // return base64-encoded string from encrypted byte array:
    Base64.getEncoder.encodeToString(encrypted)
  } // .toResult(preMsg = s"Unable encrypt string due to following error:")

  /**
   * Decrypts string value using AES256 algorithm. Uses user-defined secret key for decryption.
   *
   * @param value String value to decrypt
   * @param key   User-defined secret key
   * @return Either decrypted string or a list of decryption errors.
   */
  private def decrypt(value: String, key: EncryptionKey): String = {
    val encrypted = Base64.getDecoder.decode(value)
    val aesKey = getSecretKeySpec(key)
    val (_, ivSpec) = extractIV(encrypted)

    val c = Cipher.getInstance(cipher)
    c.init(Cipher.DECRYPT_MODE, aesKey, ivSpec)

    // extract only portion of array related to encrypted string (without IV bytes)
    val cipherBytes = new Array[Byte](encrypted.length - ivLength)
    Array.copy(encrypted, ivLength, cipherBytes, 0, cipherBytes.length)

    // return decrypted data as a string:
    new String(c.doFinal(cipherBytes), StandardCharsets.UTF_8)
  } // .toResult(preMsg = "Unable to decrypt string due to following error:")

  /**
   * Recursive function used to traverse Typesafe configuration and substitute values if necessary.
   *
   * @param shouldSubstitute Function to identify if value needs to be substituted
   * @param substitutor      Function that substitutes value.
   * @param config           Configuration to traverse.
   * @return New instance of configuration with all required values being substituted.
   */
  private def substituteConfigValues(shouldSubstitute: String => Boolean,
                                     substitutor: String => String)(config: Config): Config = {

    def loop(value: ConfigValue, key: Option[String] = None): ConfigValue = value match {
      case cObj: ConfigObject => cObj.asScala.foldLeft(cObj)((c, v) => c.withValue(v._1, loop(v._2, Some(v._1))))
      case cList: ConfigList => ConfigValueFactory.fromIterable(cList.asScala.map(v => loop(v)).asJava)
      case cVal: ConfigValue => key match {
        case Some(k) => if (shouldSubstitute(k)) { // value came from object => modify it if object key matches predicate
          val originValue = cVal.unwrapped().asInstanceOf[String]
          ConfigValueFactory.fromAnyRef(substitutor(originValue))
        } else cVal
        case None => RefType.applyRef[SparkParam](cVal.unwrapped().asInstanceOf[String]) match {
          case Right(paramValue) =>
            val kv = paramValue.value.split("=", 2)
            if (shouldSubstitute(kv(0))) ConfigValueFactory.fromAnyRef(kv(0) + "=" + substitutor(kv(1))) else cVal
          case Left(_) => cVal
        }
      }
    }

    config.root().asScala.foldLeft(config)((c, v) => c.withValue(v._1, loop(v._2, Some(v._1))))
  }

  /**
   * Encrypts sensitive fields in the provided configuration
   *
   * @param config Configuration to encrypt
   * @return New configuration instance with sensitive fields being encrypted.
   */
  def encryptConfig(config: Config): Result[Config] = Try(
    substituteConfigValues(shouldEncrypt, (value: String) => encrypt(value, secret))(config)
  ).toResult(preMsg = s"Unable encrypt configuration due to following error:")

  /**
   * Decrypts sensitive fields in the provided configuration
   *
   * @param config Configuration to encrypt
   * @return New configuration instance with sensitive fields being decrypted.
   */
  def decryptConfig(config: Config): Result[Config] = Try(
    substituteConfigValues(shouldEncrypt, (value: String) => decrypt(value, secret))(config)
  ).toResult(preMsg = s"Unable decrypt configuration due to following error:")
}

