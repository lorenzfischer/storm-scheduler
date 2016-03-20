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

package ch.uzh.ddis.stools.monitoring;

/**
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class SumAggregator extends AbstractAggregator {

    /** The current aggregate value. */
    private long sum;

    public SumAggregator() {
        this.sum = 0;
    }

    protected long getSum() {
        return this.sum;
    }

    protected void setSum(long sum) {
        this.sum = sum;
    }

    @Override
    public long update(Integer taskId, Long newContribution)  {
        long earlierContribution;
        long total;

        earlierContribution = updateTaskContribution(taskId, newContribution);
        total = getSum();
        total -= earlierContribution;
        total += newContribution;
        setSum(total);

        return total;
    }

}
