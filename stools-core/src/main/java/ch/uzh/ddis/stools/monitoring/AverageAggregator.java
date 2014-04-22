/* TODO: License */
package ch.uzh.ddis.stools.monitoring;

/**
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class AverageAggregator extends SumAggregator {

    @Override
    public long update(Integer taskId, Long newContribution)  {
        return super.update(taskId, newContribution) / Math.max(1, getNumTasks());
    }

}
