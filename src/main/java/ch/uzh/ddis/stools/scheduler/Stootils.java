/* TODO: License */
package ch.uzh.ddis.stools.scheduler;

import backtype.storm.Config;
import backtype.storm.utils.ZookeeperAuthInfo;
import com.netflix.curator.framework.CuratorFramework;

import java.util.List;
import java.util.Map;

/**
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class Stootils {

    private static CuratorFramework zkClientSingleton;

    /**
     * Creates or returns the curator zookeeper client object for the given storm configuration object. According to
     * http://curator.apache.org/curator-framework instances of CuratorFramework are fully thread-safe and should be shared
     * within an application per zk-cluster. We assume that there is only one version of the storm configuration object
     * and return a singleton instance of the zkClient.
     *
     * @param stormConf the storm configuration object, which will be used to create the CuratorFramework instance in
     *                  the case that the singleton instance is null.
     * @return a singleton instance created from the first call of this method.
     */
    public static synchronized CuratorFramework getConfiguredZkClient(Map stormConf) {
        if (zkClientSingleton == null) {
            zkClientSingleton = backtype.storm.utils.Utils.newCurator(stormConf,
                    (List<String>) stormConf.get(Config.STORM_ZOOKEEPER_SERVERS),
                    stormConf.get(Config.STORM_ZOOKEEPER_PORT),
                    "/",
                    new ZookeeperAuthInfo(stormConf));
            zkClientSingleton.start();
        }

        return zkClientSingleton;
    }

}
