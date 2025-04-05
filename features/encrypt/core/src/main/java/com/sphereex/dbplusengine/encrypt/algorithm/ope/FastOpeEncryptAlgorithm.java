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

package com.sphereex.dbplusengine.encrypt.algorithm.ope;

import com.sphereex.dbplusengine.encrypt.algorithm.ope.cipher.OpeCipher;
import com.sphereex.dbplusengine.encrypt.algorithm.ope.generator.FastOpeCipherGenerator;
import com.sphereex.dbplusengine.encrypt.algorithm.ope.generator.MopeCipherGenerator;
import com.sphereex.dbplusengine.encrypt.context.EncryptColumnDataTypeContext;
import com.sphereex.dbplusengine.encrypt.context.EncryptContext;
import com.sphereex.dbplusengine.infra.database.core.metadata.database.datatype.DataTypeExtractor;
import com.sphereex.dbplusengine.infra.database.core.metadata.database.datatype.DataTypeJavaClassConverter;
import lombok.Getter;
import org.apache.commons.codec.binary.Base32;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithm;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithmMetaData;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.context.AlgorithmSQLContext;
import org.apache.shardingsphere.infra.algorithm.core.exception.AlgorithmInitializationException;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.impl.driver.jdbc.type.util.ResultSetUtils;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

/**
 * Fast ope encrypt algorithm.
 */
public class FastOpeEncryptAlgorithm implements EncryptAlgorithm {
    
    protected static final String ALPHA_KEY = "alpha-key";
    
    protected static final String FACTOR_E_KEY = "factor-e-key";
    
    protected static final String FACTOR_K_KEY = "factor-k-key";
    
    protected static final String OFFSET = "offset";
    
    protected static final String PLAIN_TEXT_BYTE_LENGTH = "plain-text-byte-length";
    
    private static final String CHARSET_NAME = "charset-name";
    
    // todo Replace with base64hex
    private static final Base32 BASE32 = new Base32(true);
    
    @Getter
    private EncryptAlgorithmMetaData metaData;
    
    private Properties props;
    
    private OpeCipher cipher;
    
    private Charset charset;
    
    @Getter
    private Map<String, Object> udfDataModel;
    
    @Override
    public void init(final Properties props) {
        this.props = props;
        double alphaKey = getAlphaKey(props);
        double factorEKey = getFactorEKey(props);
        long factorKKey = getFactorKKey(props);
        // TODO verify the correction of mope ope
        Optional<BigInteger> offset = getOffset(props);
        charset = getCharset(props);
        Optional<Integer> plainTextByteLength = Optional.ofNullable(props.getProperty(PLAIN_TEXT_BYTE_LENGTH)).map(Integer::parseInt);
        cipher = offset.map(optional -> new MopeCipherGenerator(new FastOpeCipherGenerator(alphaKey, factorEKey, factorKKey), plainTextByteLength.orElse(8), optional).generate())
                .orElseGet(() -> new FastOpeCipherGenerator(alphaKey, factorEKey, factorKKey).generate());
        udfDataModel = createUdfDataModel(alphaKey, factorEKey, factorKKey);
        // SPEX ADDED: BEGIN
        metaData = new EncryptAlgorithmMetaData(true, true, false, true, (plainCharLength, charToByteRatio) -> calculateExpansibility(plainCharLength, charToByteRatio, alphaKey, factorEKey));
        // SPEX ADDED: END
    }
    
    private double getAlphaKey(final Properties props) {
        ShardingSpherePreconditions.checkState(props.containsKey(ALPHA_KEY) && checkDouble(props.getProperty(ALPHA_KEY), 0.8D, 1D),
                () -> new AlgorithmInitializationException(this, "%s should be a random double between 0.8 and 1.", ALPHA_KEY));
        return Double.parseDouble(props.getProperty(ALPHA_KEY));
    }
    
    private double getFactorEKey(final Properties props) {
        ShardingSpherePreconditions.checkState(props.containsKey(FACTOR_E_KEY) && checkDouble(props.getProperty(FACTOR_E_KEY), 0.012D, 1D),
                () -> new AlgorithmInitializationException(this, "%s should be a random double between 0.012 and 1.", FACTOR_E_KEY));
        return Double.parseDouble(props.getProperty(FACTOR_E_KEY));
    }
    
    private long getFactorKKey(final Properties props) {
        ShardingSpherePreconditions.checkState(props.containsKey(FACTOR_K_KEY) && checkLong(props.getProperty(FACTOR_K_KEY)),
                () -> new AlgorithmInitializationException(this, "%s should be a random long.", FACTOR_K_KEY));
        return Long.parseLong(props.getProperty(FACTOR_K_KEY));
    }
    
    private Optional<BigInteger> getOffset(final Properties props) {
        ShardingSpherePreconditions.checkState(!props.containsKey(OFFSET) || props.containsKey(OFFSET) && checkBigInteger(props.getProperty(OFFSET)),
                () -> new AlgorithmInitializationException(this, "%s should be a random biginteger.", OFFSET));
        return Optional.ofNullable(props.getProperty(OFFSET)).map(BigInteger::new);
    }
    
    private Charset getCharset(final Properties props) {
        try {
            return Charset.forName(props.getProperty(CHARSET_NAME, StandardCharsets.UTF_8.name()));
        } catch (final UnsupportedCharsetException ex) {
            throw new AlgorithmInitializationException(this, "Unsupported charset: %s", props.getProperty(CHARSET_NAME));
        }
    }
    
    private boolean checkDouble(final String randomDouble, final double origin, final double bound) {
        double parsedDouble;
        try {
            parsedDouble = Double.parseDouble(randomDouble);
        } catch (final NullPointerException | NumberFormatException ignore) {
            return false;
        }
        return parsedDouble > origin && parsedDouble < bound;
    }
    
    private boolean checkLong(final String randomLong) {
        if (null == randomLong) {
            return false;
        }
        try {
            Long.parseLong(randomLong);
        } catch (final NumberFormatException ignore) {
            return false;
        }
        return true;
    }
    
    private boolean checkBigInteger(final String offset) {
        if (null == offset) {
            return true;
        }
        try {
            new BigInteger(offset);
        } catch (final NumberFormatException ignore) {
            return false;
        }
        return true;
    }
    
    private Map<String, Object> createUdfDataModel(final double randomDouble, final double randomDouble2, final long randomLong) {
        Map<String, Object> result = new HashMap<>();
        result.put("randomDouble", randomDouble);
        result.put("randomDouble2", randomDouble2);
        result.put("randomLong", randomLong);
        return result;
    }
    
    // todo Just persist encrypted binary data to the database
    @Override
    public Object encrypt(final Object plainValue, final AlgorithmSQLContext algorithmSQLContext, final EncryptContext encryptContext) {
        if (null == plainValue) {
            return null;
        }
        Optional<Class<?>> plaintextJavaType = getConfiguredConvertClass(encryptContext);
        if (plaintextJavaType.isPresent()) {
            return encrypt(plainValue, plaintextJavaType.get());
        }
        return BASE32.encodeAsString(cipher.encryptString(plainValue.toString(), charset));
    }
    
    private Object encrypt(final Object plainValue, final Class<?> plaintextJavaType) {
        Optional<Function<Object, byte[]>> encryptFunction = cipher.getSupportedEncryptFunction(plaintextJavaType, charset);
        if (encryptFunction.isPresent()) {
            try {
                Object convertedValue = ResultSetUtils.convertValue(plainValue, plaintextJavaType);
                return BASE32.encodeAsString(encryptFunction.get().apply(convertedValue));
            } catch (final SQLFeatureNotSupportedException ignored) {
            }
        }
        return BASE32.encodeAsString(cipher.encryptString(plainValue.toString(), charset));
    }
    
    @Override
    public Object decrypt(final Object cipherValue, final AlgorithmSQLContext algorithmSQLContext, final EncryptContext encryptContext) {
        if (null == cipherValue) {
            return null;
        }
        byte[] bytes = BASE32.decode(cipherValue.toString());
        Optional<Class<?>> plaintextJavaType = getConfiguredConvertClass(encryptContext);
        if (plaintextJavaType.isPresent()) {
            Optional<Function<byte[], Object>> decryptFunction = cipher.getSupportedDecryptFunction(plaintextJavaType.get(), charset);
            return decryptFunction.map(optional -> optional.apply(bytes)).orElseGet(() -> cipher.decryptString(bytes, charset));
        }
        return cipher.decryptString(bytes, charset);
    }
    
    private Optional<Class<?>> getConfiguredConvertClass(final EncryptContext encryptContext) {
        EncryptColumnDataTypeContext columnDataType = encryptContext.getColumnDataType();
        if (null == columnDataType || null == columnDataType.getLogicDataType()) {
            return Optional.empty();
        }
        String dataType = DataTypeExtractor.extract(columnDataType.getLogicDataType(), encryptContext.getDatabaseType());
        return DataTypeJavaClassConverter.getDataTypeJavaClass(encryptContext.getDatabaseType(), dataType);
    }
    
    private int calculateExpansibility(final int plainTextCharLength, final int charToByteRatio, final double alphaKey, final double factorEKey) {
        int plainByteLength = plainTextCharLength * charToByteRatio;
        double factorN = 16 / ((1 - alphaKey / 2) * factorEKey * Math.pow(alphaKey / 2.0, 8));
        int cipherBits = Integer.toBinaryString((int) Math.ceil(factorN)).length();
        int plaintextBytesPerBlock = cipherBits % 8;
        int rate = ((plainByteLength + plaintextBytesPerBlock - 1) / plaintextBytesPerBlock * (plaintextBytesPerBlock * cipherBits / 8) + 1) / plainByteLength;
        return (rate * plainByteLength) << 1;
    }
    
    @Override
    public AlgorithmConfiguration toConfiguration() {
        return new AlgorithmConfiguration(getType(), props);
    }
    
    @Override
    public String getType() {
        return "SphereEx:FASTOPE";
    }
}
