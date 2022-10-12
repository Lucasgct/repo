/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;

import javax.crypto.SecretKey;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

public class ExtensionTokenProcessorTests extends OpenSearchTestCase {

    private static final String userName = "user1";
    private static final Principal userPrincipal = () -> userName; 

    public void testGenerateToken() {

        System.out.println("Start of the extension token tests");
        String extensionUniqueId = "ext_1";
        ExtensionTokenProcessor extensionTokenProcessor = new ExtensionTokenProcessor(extensionUniqueId);

        PrincipalIdentifierToken generatedIdentifier;

        try {
            generatedIdentifier = extensionTokenProcessor.generateToken(userPrincipal);
        } catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException
                | InvalidAlgorithmParameterException | IllegalBlockSizeException | BadPaddingException
                | IOException e) {
       
            System.out.println("Token Generation Test Failed");
            e.printStackTrace();
            throw new Error(e);
        }

        assertNotEquals(null, generatedIdentifier);
        System.out.println(generatedIdentifier.getToken());
    }

    public void testExtractPrincipal() {
        String extensionUniqueId = "ext_2";

        ExtensionTokenProcessor extensionTokenProcessor = new ExtensionTokenProcessor(extensionUniqueId);

        PrincipalIdentifierToken generatedIdentifier;

        String principalName;

        SecretKey secretKey; 

        try {
            generatedIdentifier = extensionTokenProcessor.generateToken(userPrincipal);
            secretKey = extensionTokenProcessor.getSecretKey(); 
            principalName = extensionTokenProcessor.extractPrincipal(generatedIdentifier, secretKey);
        } catch (InvalidKeyException | IllegalArgumentException | InvalidAlgorithmParameterException
                | IllegalBlockSizeException | BadPaddingException | NoSuchAlgorithmException
                | NoSuchPaddingException | IOException e) {
           
            System.out.println("Name extraction or ID generation failed");
            e.printStackTrace();
            throw new Error(e);
                }
        

        assertEquals(userName, principalName);
    }


    // public void testExtractPrincipalWithNullToken() {
    //     String extensionUniqueId1 = "ext_1";
    //     ExtensionTokenProcessor extensionTokenProcessor = new ExtensionTokenProcessor(extensionUniqueId1);

    //     Exception exception = assertThrows(IllegalArgumentException.class, () -> extensionTokenProcessor.extractPrincipal(null));

    //     assertFalse(exception.getMessage().isEmpty());
    //     assertEquals(ExtensionTokenProcessor.INVALID_TOKEN_MESSAGE, exception.getMessage());
    // }

    // public void testExtractPrincipalWithTokenInvalidExtension() {
    //     String extensionUniqueId1 = "ext_1";
    //     String extensionUniqueId2 = "ext_2";
    //     String token = userPrincipal.getName() + ":" + extensionUniqueId1;
    //     PrincipalIdentifierToken principalIdentifierToken = new PrincipalIdentifierToken(token);
    //     ExtensionTokenProcessor extensionTokenProcessor = new ExtensionTokenProcessor(extensionUniqueId2);

    //     Exception exception = assertThrows(
    //         IllegalArgumentException.class,
    //         () -> extensionTokenProcessor.extractPrincipal(principalIdentifierToken)
    //     );

    //     assertFalse(exception.getMessage().isEmpty());
    //     assertEquals(ExtensionTokenProcessor.INVALID_EXTENSION_MESSAGE, exception.getMessage());
    // }
}
