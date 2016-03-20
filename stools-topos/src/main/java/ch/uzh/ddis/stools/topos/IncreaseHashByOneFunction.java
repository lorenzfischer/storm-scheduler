/**
 *  @author Lorenz Fischer
 *
 *  Copyright 2016 University of Zurich
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package ch.uzh.ddis.stools.topos;

import backtype.storm.tuple.Values;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;
import java.util.UUID;

/**
 * This function takes the String value of the first field, computes it's hash value, and emits a new value that
 * has a hash/mod value that is greater than the hash/mod value of the previous String value by 1.
 * <p/>
 * For example, if the contents of the first field would hash to 12 and this class has been initialized for 10
 * partitions, the hash/mod value for the first field would be 3. This function will then compute a new value to emit
 * that has a hash/mod value of 3. For this, we generate random values until we find a value with the required
 * hash/mod value.
 *
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class IncreaseHashByOneFunction implements Function {

    private final int numPartitions;
    private final int maxTry;

    public IncreaseHashByOneFunction(int numPartitions) {
        this.numPartitions = numPartitions;
        this.maxTry = this.numPartitions * 100;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String currentValue;
        int newHashModValue;
        String newValue;
        int currentTry;

        currentTry = 0;
        currentValue = tuple.getString(0);
        newHashModValue = (currentValue.hashCode() % this.numPartitions) + 1;
        newHashModValue = newHashModValue % this.numPartitions;  // rotate back to 0 if necessary

        do {
            newValue = UUID.randomUUID().toString();

            if (currentTry++ >= maxTry) {
                throw new RuntimeException("Couldn't find a value for hash/mod " + newHashModValue +
                        " in " + maxTry + " attempts. Current value is " + newValue +
                        " Number of partitions: " + this.numPartitions);
            }

        } while ((newValue.hashCode() % this.numPartitions) != newHashModValue);

        collector.emit(new Values(newValue));
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

    }

    @Override
    public void cleanup() {

    }
}
