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

package ch.uzh.ddis.stools.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

/**
 * This class hides all the hacky details of how we collect information about the network interfaces.
 *
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public final class NetStat { // util classes should not be inherited from

    /**
     * This is the command we execute to get the network statistics.
     */
    private static final String CMD_NETSTAT = "netstat -ib";

    /**
     * The name pattern vor all network interfaces that are non-loopback interfaces.
     */
    private static final String IF_PATTERN = "en[0-9]+";

    /**
     * @return the number of bytes transmitted over all network interfaces other than the loopback interface.
     * On a mac this will capture all network interfaces other than the loop-back interface.
     * @throws java.io.IOException if something goes wrong when calling the system command to retrieve the network
     *                             statistics.
     */
    public static long getTotalTransmittedBytes() throws IOException {
        /*
            This method will call the system utility "netstat -ib" which (on a mac) will return an output similar to:

            Name  Mtu   Network       Address            Ipkts Ierrs     Ibytes    Opkts Oerrs     Obytes  Coll
            lo0   16384 <Link#1>                       2183334     0  295602781  2183334     0  295602781     0
            lo0   16384 localhost   ::1                2183334     -  295602781  2183334     -  295602781     -
            lo0   16384 127           localhost        2183334     -  295602781  2183334     -  295602781     -
            lo0   16384 localhost   fe80:1::1          2183334     -  295602781  2183334     -  295602781     -
            gif0* 1280  <Link#2>                             0     0          0        0     0          0     0
            stf0* 1280  <Link#3>                             0     0          0        0     0          0     0
            en0   1500  <Link#5>    54:26:96:da:ed:9f   114193     0   75981699   169308     0   30717122     0
            en4   1500  <Link#6>    32:00:18:8b:1b:80        0     0          0        0     0          0     0
            en5   1500  <Link#7>    32:00:18:8b:1b:81        0     0          0        0     0          0     0
            bridg 1500  <Link#8>    22:c9:d0:b2:ac:00        0     0          0        1     0        342     0
            p2p0* 2304  <Link#9>    06:26:96:da:ed:9f        0     0          0        0     0          0     0
            en2   1500  <Link#4>    20:c9:d0:2b:94:e5 11771286     1 11116599989  7380954     0 2255963348     0
            en2   1500  macbookpro- fe80:4::22c9:d0ff 11771286     - 11116599989  7380954     - 2255963348     -
            en2   1500  130.60.156/24 130.60.156.85   11771286     - 11116599989  7380954     - 2255963348     -

            We parse this output by reading it line by line, and adding up all "Ibytes" and "Obytes" records for all
            network interfaces having a name that matches the pattern "en[0-9]+".
        */

        BufferedReader reader;
        String line; // the current line we're processing
        Set<String> processedInterfaces; // prevent adding the numbers for the same interface multiple times
        long inBytes, outBytes;

        processedInterfaces = new HashSet<>();
        reader = new BufferedReader(new InputStreamReader(Runtime.getRuntime().exec(CMD_NETSTAT).getInputStream()));
        inBytes = outBytes = 0L;

        while ((line = reader.readLine()) != null) {
            String[] parts;
            String ifName;

            parts  = line.split("\\s+");
            ifName = parts[0];

            if (!processedInterfaces.contains(ifName) && ifName.matches(IF_PATTERN)) {
                inBytes += Long.parseLong(parts[6]);
                outBytes += Long.parseLong(parts[9]);
                processedInterfaces.add(ifName);
            }
        }

        return inBytes + outBytes;
    }

}
