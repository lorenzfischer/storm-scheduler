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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is used by the metrics consumer class to aggregate (sum up) the metrics values across all tasks. It does
 * this by keeping both, the current total value as well as the values for each task. Whenever a new task-value is added
 * to the total, the value previously stored for the given task is substracted from the total. This way the updates and
 * the lookups have constant time complexity.
 * <p/>
 * This class is not thread-safe. Access from multiple threads should be synchronized.
 *
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public abstract class AbstractAggregator {

    /**
     * This map contains the metric value for each task. key=taskId, value=value. We use a map, because there can also
     * be negative taskIds which renders it complicated to use an arraylist.
     */
    private Map<Integer, Long> taskValues;

    /**
     * Constructs a new aggregator object.
     */
    public AbstractAggregator() {

        this.taskValues = new HashMap<Integer, Long>() {
            @Override
            public Long get(Object key) {
                Long result = super.get(key);
                if (result == null) {
                    result = Long.valueOf(0L);
                }
                return result;
            }
        };

    }

    /**
     * Updates the value for the given task. In case that this task has previously contributed to the current
     * total, the previous contribution needs to be removed from the current aggregate total before the new contribution
     * is added.
     *
     * @param taskId the id of the task, contributing a value.
     * @param newContribution the value that should be incorporated in the running total.
     * @return the new running total, currently stored in this object.
     */
    abstract public long update(Integer taskId, Long newContribution);

    /**
     * @return the number of tasks, this aggregator contains values from.
     */
    public int getNumTasks() {
        return this.taskValues.size();
    }

    /**
     * Looks up and updates the contribution that a task has made to the current aggregate.
     * @param taskId the task whose contribution should be looked up.
     * @param newValue the new contribution for this task.
     * @return the old contribution.
     */
    protected long updateTaskContribution(Integer taskId, long newValue) {
        long result;

        result = this.taskValues.get(taskId).longValue();
        this.taskValues.put(taskId, newValue);

        return result;
    }
}
