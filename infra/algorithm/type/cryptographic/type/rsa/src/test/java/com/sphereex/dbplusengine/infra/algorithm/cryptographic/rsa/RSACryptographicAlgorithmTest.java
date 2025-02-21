/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sphereex.dbplusengine.infra.algorithm.cryptographic.rsa;

import org.apache.shardingsphere.infra.algorithm.core.exception.AlgorithmInitializationException;
import org.apache.shardingsphere.infra.algorithm.cryptographic.core.CryptographicAlgorithm;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.test.util.PropertiesBuilder;
import org.apache.shardingsphere.test.util.PropertiesBuilder.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RSACryptographicAlgorithmTest {
    
    private static final String RSA = "SphereEx:RSA";
    
    private static final String RSA_PUBLIC_KEY = getPublicKey();
    
    private static final String RSA_PRIVATE_KEY = getPrivateKey();
    
    private CryptographicAlgorithm encryptAlgorithm;
    
    @BeforeEach
    void setUp() {
        encryptAlgorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, RSA, createProps());
    }
    
    @Test
    void assertEncrypt() {
        Object actual = encryptAlgorithm.encrypt("test1234");
        assertThat(actual, is(getEncryptText()));
    }
    
    @Test
    void assertIllegalKeys() {
        String publicKey = "1";
        String privateKey = "1";
        assertThrows(AlgorithmInitializationException.class, () -> TypedSPILoader.getService(CryptographicAlgorithm.class, RSA,
                PropertiesBuilder.build(new PropertiesBuilder.Property("sm2-public-key-value", publicKey), new PropertiesBuilder.Property("sm2-private-key-value", privateKey))));
    }
    
    @Test
    void assertEncryptNullValue() {
        assertNull(encryptAlgorithm.encrypt(null));
    }
    
    @Test
    void assertEncryptWithLongValue() {
        Object actual = encryptAlgorithm.encrypt(getLongText());
        assertThat(actual, is(getLongEncryptText()));
    }
    
    private Properties createProps() {
        return PropertiesBuilder.build(new Property("rsa-public-key-value", RSA_PUBLIC_KEY), new Property("rsa-private-key-value", RSA_PRIVATE_KEY));
    }
    
    @Test
    void assertDecrypt() {
        Object actual = encryptAlgorithm.decrypt(getEncryptText());
        assertThat(actual, is("test1234"));
    }
    
    @Test
    void assertDecryptWithLongValue() {
        Object actual = encryptAlgorithm.decrypt(getLongEncryptText());
        assertThat(actual, is(getLongText()));
    }
    
    @Test
    void assertDecryptWithNullValue() {
        Object actual = encryptAlgorithm.decrypt(null);
        assertNull(actual);
    }
    
    @Test
    void assertDecryptWithEmptyValue() {
        Object actual = encryptAlgorithm.decrypt("");
        assertThat(actual, is(""));
    }
    
    @Test
    void assertEncryptAndDecrypt() {
        encryptAndDecrypt(encryptAlgorithm, "test");
        encryptAndDecrypt(encryptAlgorithm, "testRSA");
        encryptAndDecrypt(encryptAlgorithm, "testRSA中文");
        encryptAndDecrypt(encryptAlgorithm, "testRSA中文?&!*@#");
    }
    
    private void encryptAndDecrypt(final CryptographicAlgorithm algorithm, final String plainValue) {
        Object encryptedValue = algorithm.encrypt(plainValue);
        Object decryptedValue = algorithm.decrypt(encryptedValue);
        assertThat(decryptedValue.toString(), is(plainValue));
    }
    
    private String getLongText() {
        return "Twinkle, twinkle, little star,\n"
                + "How I wonder what you are!\n"
                + "Up above the world so high,\n"
                + "Like a diamond in the sky!\n"
                + "Twinkle, twinkle, little star,\n"
                + "How I wonder what you are!\n"
                + "When the blazing sun is gone,\n"
                + "When he nothing shines upon,\n"
                + "Then you show your little light,\n"
                + "Twinkle, twinkle, all the night.\n";
    }
    
    private String getEncryptText() {
        return "Nu2gcC/KXrlGjG+0AbrjM3R90jTGf7DlGT4AUnjKs1jb892JJELvpIdp/UdUpPeEG377EseQbL92XxSJF331d895muQWa7m/zEwHmgUt"
                + "U8S6rTxZ++Fxsegc618d71LgnihMg6Tf/DBAM17K43a7+fo7MRoQabowIDi1plTWa49ndN9UMVnLSX+Kxd4qfPdSt1Zc/QWDeqPperq"
                + "RBCJiu8LIzLkUf5gW5B9c8Vk/EYHgMQsMHiMmCQWYIdiRfXQZyaqaTcfNmnZCIV8gVJOLXx6oEe1PwFQgw1sok4V/qhaYbevj+4gr3R6k/4T2zGsz1mRMJn4qMJmDk9gVGZnOOA==";
    }
    
    private static String getLongEncryptText() {
        return "YJXUIjZW1MG8cr5XtjgTuEwjCRao6C1xIMgwGE0zGqArr0qC6xLcPi/6mop2aQ6iq3SDVMNBxshwbnkZyLUnK1RtLOmPrbfc1gAtoVGv2oJzP8cOymLZqIOguEbhuU1zd3ODpbgO9BGvaVLzHL486fKmEDO0lF/aSs9LNN5lmRuVxyW6vB"
                + "I5ZgWOmXa9XLW5khPxuG8ytGJ5/rRfuUW5l7U8/sX1s8bjbU/quhnMX1/HoAtwwWFxb6Bh3MG16Kj99F46qZ3fUAcFsyjSNoNKts2Z6zQNkhuYhkpSBagfIb36PaWR9Fjs6FbJUez2WQaElBYfIQfwWMeUc1AVoqO4xQs1YwmGgd/hBwG"
                + "zaCYVKwb9xFQdKfqQm1Q+9whF43TGyx/5dVvYEHpouRhTwRblDb5mY5Z1sd3B0kVc9dLXQS9uwgHE+aoA0xKduf4v3X01L5XeUAxSTY0oH1/vJWevTTX4WDMs0szyYTvst1PmEjVqM0Le3rryxCm2paanNmVT0uAUtPxajBX6UMd69IxE"
                + "3UbG4lvMK7lrVa04pB3pA/NpSMgGj6svbAUYxe9XngKWA2y7aBeUD2RMnQI56U94Aa6gVKqO+v9vlrfFMkGdgTsKUB/f9rrS01WxVBCI4iIQyHkaOB3SV864PgnCYwdlo0b8iade2M+4A76K68I3FJM=";
    }
    
    private static String getPublicKey() {
        return "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAnkOZVTuURVur"
                + "YRbFIjtWwSEcCyJ3vwlvlwUEAFsbp0Ub0toFAlkPZ+lSI1xyOw0u0bs/kxItk6kRkOUJcPf9nXCrLHEpxoeN9QBhU86k5c/M2g2"
                + "C5iIyiJXTYz0R0ddYJWIuz7jXimnQP7uHoLR8PUAT9DMhcG3lK5cT4LDiiqAvuxhVJzlNIDPFJ5U0/XdpJ5oKzJYVal/10pYT0N"
                + "t5prx3tFl0uwTgw+NhRWx7IssNfwiqKaD8Cq9/lwb0gtXNxmL8KDCG/8TNj+bfJQD+9y0lQXIenUx+JYCXc32NA9Pb7Js8SHyAvmpkreu4XYzMk4so+OCb1SIxBhbIM1tvuQIDAQAB";
    }
    
    private static String getPrivateKey() {
        return "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCeQ5lVO5RFW6thFsUiO1bBIRwLIne/CW+XBQQAWxunRRvS2gUCWQ9n6VI"
                + "jXHI7DS7Ruz+TEi2TqRGQ5Qlw9/2dcKsscSnGh431AGFTzqTlz8zaDYLmIjKIldNjPRHR11glYi7PuNeKadA/u4egtHw9QBP0My"
                + "FwbeUrlxPgsOKKoC+7GFUnOU0gM8UnlTT9d2knmgrMlhVqX/XSlhPQ23mmvHe0WXS7BODD42FFbHsiyw1/CKopoPwKr3+XBvSC1"
                + "c3GYvwoMIb/xM2P5t8lAP73LSVBch6dTH4lgJdzfY0D09vsmzxIfIC+amSt67hdjMyTiyj44JvVIjEGFsgzW2+5AgMBAAECggEA"
                + "QgmO54OuwKoZfq+Tnk8IShnYq8S8FpiHWYqcOtHJXih2DasvP+WNihxPS7X9bCp6CMWLJ4EER4Lac40+PUcdKh6jLi4h8lcJott/"
                + "wQqOv93PaoUMw54tW9S4mcYXs2mZvC+VvNMyDO1OGenUE/h7hstACDt1joYsg93MS4tDW/gHC9eP7hDANZrHs7db26iiP6D8b7G"
                + "jB+XL6QFgN1q+o6qXmqzvpePGRj4GYuootpDkuxZoDv3GH1gzHoOQnuPMfOm7cE5ov6DrC7OFNgNKDqn5pvo215RL8yBfJ4a6E3"
                + "2AuOzbjtgASX8x7RQQAIHCuEMU75JO+BDw39QqSszsKQKBgQDSnM+/pGztII/IUyFdtSGoJxu2wDoBEMUshJFLSddtvjBH8tqmT"
                + "GVIl20TbpB4C4sItj2wpMKEzbjs4+wmw+a7h/tR0NHlGEY7pXgNY9QPOjY8kQJeCsVPMUZqqprT2329xm9OUXdgSgaMgvhfaMvR"
                + "wU+/ufs6UwtyMIwsKFU2rwKBgQDAXswiWVufUQn/RyiY2bb2+lwG8MLhgKv+ntV/CF0+O2d6Pi+HsFPzrEgNYsWCrnYA9wv9McJ"
                + "Qi78mBkQYmKQvWfaTMwLOCLsdK/7icPFSWlACRlDOBHc8XXdliQD29dNaqG6BDkwAZ9AgBjKHEMjQMExmgYnR+VFlWX3bp6ZaFw"
                + "KBgBtZ8ATkVp0I8INEgH3J6yAKTCgUmLPQuLqKUNAlO8vtuhlt6YVVQIYH3Et8vVhJr3mnKSXKj9RtXwmso9t473sFMtcyNj/5Q"
                + "g229HtQrpZ3qdl9v3/1CCC7tnhdxZOj2pWNsqDKJaWkl2siCx1g369S2od8oKq3ZDIlKd8GMeLTAoGBAJ/+eBNddImV0hXCLi6q"
                + "bLUPVvjix4LcDLCxk+maoEqBB7gw/kEBU2GH+UlAy/q7dSOqVQtZlj59bBaJAZvfYDaNwTl+JKgNtOo3TD8zJlKTEJZDuzMNncn"
                + "UBtio0OeVXxq4mWe251ky/nOUE/Qn7ozQjsp2lJTRonQDsVy+G+ozAoGBAISp1Hb/cWfZWpi2P6+UlN4PJhitpNam6se2PDbQhe"
                + "Un782OdWt9uV8HmeRscXPvobb1nD2pY8mKs32GNAzqikoVzfNcH2nxwopcz+z6WsrUzeLtdEFk9ABkDAAtQnlPpfnjLZVt5rc0I"
                + "s7VZ9kDmiXiUOUN7eiQeaXnGykH7B5/";
    }
}
