/**
* Sclera - Encryption Manager
* Copyright 2012 - 2020 Sclera, Inc.
* 
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* 
*     http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.scleradb.util.encrypt

import java.security.MessageDigest

import javax.crypto.{KeyGenerator, SecretKey, Cipher}
import javax.crypto.spec.{SecretKeySpec, IvParameterSpec}

import org.apache.commons.codec.binary.Base64
 
/** Data encryptor */
class Encryptor(encryptionKey: String) {
    private def flip(mode: Int, bytes: Array[Byte]): Array[Byte] = {
        val cipher: Cipher = Cipher.getInstance("AES")
        val key: SecretKeySpec =
            new SecretKeySpec(encryptionKey.getBytes("UTF-8"), "AES")

        cipher.init(mode, key)
        cipher.doFinal(bytes)
    }
 
    /** Encrypt the given text string */
    def encrypt(plainText: String): String = {
        val cipherBytes: Array[Byte] =
            flip(Cipher.ENCRYPT_MODE, plainText.getBytes)
        Base64.encodeBase64String(cipherBytes)
    }

    /** Decrypt the given cipher */
    def decrypt(cipherText: String): String = {
        val cipherBytes: Array[Byte] = Base64.decodeBase64(cipherText)
        new String(flip(Cipher.DECRYPT_MODE, cipherBytes))
    }
}

object Encryptor {
    def apply(encryptionKey: String) = new Encryptor(encryptionKey)
}
