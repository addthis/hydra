/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.addthis.hydra.data.filter.value;

import java.util.Random;

import java.text.DecimalFormat;

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">returns a random number</span>.
 * <p/>
 * <p>The input value to this filter is ignored. By default, this filter produces a random
 * number from a uniform distribution in the range (0, {@link #max max}]. A long value is
 * returned if the {@link #asLong asLong} field is enabled. A floating point value is returned
 * if the {@link #asFloat asFloat} field is enabled. A value from a guassian distribution
 * can be generated using the {@link #gaussian gaussian} field.</p>
 * <p/>
 * <p>Example:</p>
 * <pre>
 *     {random {asLong:true, max:100}}
 * </pre>
 *
 * @user-reference
 */
public class ValueFilterRandom extends AbstractValueFilter {

    private static final Object lock = new Object();
    private final Random random = new Random();

    /**
     * The number of digits to produce if the output is a string.
     */
    @FieldConfig(codable = true)
    private int size = 7;

    /**
     * If gaussian is false, then this is the upper bound for the uniform distribution. Default
     * is 1,000,000.
     */
    @FieldConfig(codable = true)
    private int max = 1000000;

    /**
     * If true, then generate a double value from a normal (or gaussian) distribution. Default is
     * false.
     */
    @FieldConfig(codable = true)
    private boolean gaussian;

    /**
     * If true, then return a long value from the uniform distribution [0, max). Default is false.
     */
    @FieldConfig(codable = true)
    private boolean asLong;

    /**
     * The mean value for the gaussian distribution.
     */
    @FieldConfig(codable = true)
    private int mu;

    /**
     * The standard deviation for the gaussian distribution.
     */
    @FieldConfig(codable = true)
    private float sigma;

    /**
     * If true, then return a floating point number from the uniform distribution [0,
     * 1). Default is false.
     */
    @FieldConfig(codable = true)
    private boolean asFloat;

    private DecimalFormat format;

    @Override
    public ValueObject filterValue(ValueObject value) {
        if (gaussian) {
            synchronized (lock) {
                return ValueFactory.create(nextGaussian(mu, sigma));
            }
        }
        if (format == null) {
            format = new DecimalFormat("000000000000".substring(8 - size));
        }
        if (asFloat) {
            float frand = random.nextFloat();
            return ValueFactory.create(frand);
        }
        long rand = Math.abs(random.nextLong()) % max;
        if (asLong) {
            return ValueFactory.create(rand);
        }
        synchronized (format) {
            return ValueFactory.create(format.format(rand));
        }
    }

    // from org.apache.cassandra.contrib.stress.util.OperationThread
    protected Double nextGaussian = null;

    /**
     * Gaussian distribution.
     *
     * @param mu    is the mean
     * @param sigma is the standard deviation
     * @return next Gaussian distribution number
     */
    private double nextGaussian(int mu, float sigma) {
        // Random random = Stress.randomizer;
        Double currentState = nextGaussian;

        if (currentState == null) {
            double x2pi = random.nextDouble() * 2 * Math.PI;
            double g2rad = Math.sqrt(-2.0 * Math.log(1.0 - random.nextDouble()));

            currentState = Math.cos(x2pi) * g2rad;
            nextGaussian = Math.sin(x2pi) * g2rad;
        }

        return mu + currentState * sigma;
    }
}
