package ru.raiffeisen.checkita.config.appconf


import ru.raiffeisen.checkita.config.RefinedTypes.EncryptionKey

/**
 * Application-level configuration describing encryption sensitive fields
 *
 * @param secret Secret string used to encrypt/decrypt sensitive fields
 * @param keyFields List of key fields used to identify fields that requires encryption/decryption.
 */
final case class Encryption(
                             secret: EncryptionKey,
                             keyFields: Seq[String] = Seq("password", "secret")
                           )
