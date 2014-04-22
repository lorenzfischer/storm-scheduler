/* TODO: License */
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
