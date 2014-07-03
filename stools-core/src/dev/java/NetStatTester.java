/* TODO: License */

import ch.uzh.ddis.stools.utils.NetStat;

import java.io.IOException;

/**
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class NetStatTester {

    public static void main(String... args) {
        try {
            System.out.println(Long.toString(NetStat.getTotalTransmittedBytes()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
