package com.streamlens.service.security;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.spec.KeySpec;
import java.util.Arrays;
import java.util.Base64;

/**
 * Provides AES-256-GCM encryption for sensitive data like connection credentials.
 * Uses PBKDF2 for key derivation with a machine-specific salt.
 */
@Service
public class EncryptionService {

    private static final String ALGORITHM = "AES/GCM/NoPadding";
    private static final int GCM_IV_LENGTH = 12;
    private static final int GCM_TAG_LENGTH = 128;
    private static final int SALT_LENGTH = 16;

    private final int keyIterations;
    private final int keyLength;
    private final SecureRandom secureRandom;

    public EncryptionService(
            @Value("${streamlens.security.key-iterations:100000}") int keyIterations,
            @Value("${streamlens.security.key-length:256}") int keyLength) {
        this.keyIterations = keyIterations;
        this.keyLength = keyLength;
        this.secureRandom = new SecureRandom();
    }

    /**
     * Encrypts plaintext using AES-256-GCM with a derived key.
     *
     * @param plaintext The text to encrypt
     * @param password  The password for key derivation
     * @return Base64-encoded ciphertext (salt + IV + encrypted data)
     */
    public String encrypt(String plaintext, String password) {
        try {
            // Generate salt and IV
            byte[] salt = new byte[SALT_LENGTH];
            byte[] iv = new byte[GCM_IV_LENGTH];
            secureRandom.nextBytes(salt);
            secureRandom.nextBytes(iv);

            // Derive key from password
            SecretKey key = deriveKey(password, salt);

            // Encrypt
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
            cipher.init(Cipher.ENCRYPT_MODE, key, spec);
            byte[] ciphertext = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));

            // Combine salt + IV + ciphertext
            ByteBuffer buffer = ByteBuffer.allocate(SALT_LENGTH + GCM_IV_LENGTH + ciphertext.length);
            buffer.put(salt);
            buffer.put(iv);
            buffer.put(ciphertext);

            return Base64.getEncoder().encodeToString(buffer.array());
        } catch (Exception e) {
            throw new EncryptionException("Failed to encrypt data", e);
        }
    }

    /**
     * Decrypts Base64-encoded ciphertext using AES-256-GCM.
     *
     * @param encryptedData Base64-encoded ciphertext (salt + IV + encrypted data)
     * @param password      The password for key derivation
     * @return Decrypted plaintext
     */
    public String decrypt(String encryptedData, String password) {
        try {
            byte[] decoded = Base64.getDecoder().decode(encryptedData);
            ByteBuffer buffer = ByteBuffer.wrap(decoded);

            // Extract salt, IV, and ciphertext
            byte[] salt = new byte[SALT_LENGTH];
            byte[] iv = new byte[GCM_IV_LENGTH];
            byte[] ciphertext = new byte[decoded.length - SALT_LENGTH - GCM_IV_LENGTH];

            buffer.get(salt);
            buffer.get(iv);
            buffer.get(ciphertext);

            // Derive key from password
            SecretKey key = deriveKey(password, salt);

            // Decrypt
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
            cipher.init(Cipher.DECRYPT_MODE, key, spec);
            byte[] plaintext = cipher.doFinal(ciphertext);

            return new String(plaintext, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new EncryptionException("Failed to decrypt data", e);
        }
    }

    /**
     * Derives a secret key from a password using PBKDF2.
     */
    private SecretKey deriveKey(String password, byte[] salt) throws Exception {
        SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
        KeySpec spec = new PBEKeySpec(password.toCharArray(), salt, keyIterations, keyLength);
        byte[] keyBytes = factory.generateSecret(spec).getEncoded();

        // Clear the spec for security
        try {
            SecretKey key = new SecretKeySpec(keyBytes, "AES");
            return key;
        } finally {
            Arrays.fill(keyBytes, (byte) 0);
        }
    }

    /**
     * Generates a machine-specific default password based on system properties.
     * Used when no master password is configured.
     */
    public String getMachineKey() {
        String username = System.getProperty("user.name", "default");
        String hostname = getHostname();
        String userHome = System.getProperty("user.home", "/");
        return username + ":" + hostname + ":" + userHome.hashCode();
    }

    private String getHostname() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "localhost";
        }
    }

    /**
     * Custom exception for encryption operations.
     */
    public static class EncryptionException extends RuntimeException {
        public EncryptionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
