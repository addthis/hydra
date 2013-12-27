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
package com.addthis.hydra.data.util;

import java.util.List;
import java.util.TreeMap;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Date: 10/30/12
 */
public class TestTrendingScore {

    @Test
    public void testTrendingScore() {
        TrendingScore trendingScore = new TrendingScore();

        // Add some hourly trends
        TreeMap<String, KeyTopper> hourly = new TreeMap<String, KeyTopper>();
        KeyTopper kt1 = new KeyTopper().init().setLossy(true);
        kt1.increment("foo", 1, 10);
        kt1.increment("bar", 1, 10);
        KeyTopper kt2 = new KeyTopper().init().setLossy(true);
        kt2.increment("foo", 1, 10);
        kt2.increment("bar", 2, 10);
        KeyTopper kt3 = new KeyTopper().init().setLossy(true);
        kt3.increment("foo", 1, 10);
        kt3.increment("bar", 1, 10);
        kt3.increment("foobar", 2, 10);
        KeyTopper kt4 = new KeyTopper().init().setLossy(true);
        kt4.increment("foo", 1, 10);
        kt4.increment("bar", 2, 10);
        kt3.increment("foobar", 4, 10);

        hourly.put("20120901", kt1);
        hourly.put("20120902", kt2);
        hourly.put("20120903", kt3);
        hourly.put("20120904", kt4);

        // add some monthly trends
        TreeMap<String, KeyTopper> monthly = new TreeMap<String, KeyTopper>();
        KeyTopper kt5 = new KeyTopper().init().setLossy(true);
        kt5.increment("some", 1, 10);
        kt5.increment("more", 2, 10);
        KeyTopper kt6 = new KeyTopper().init().setLossy(true);
        kt6.increment("foo", 1, 10);
        kt6.increment("more", 2, 10);
        kt6.increment("urls", 4, 10);

        monthly.put("201209", kt5);
        monthly.put("201210", kt6);

        // now calculate trending scores
        List<URLTree.TreeObject.TreeValue> values = trendingScore.getTrends(hourly, monthly, null);

        assertEquals(values.toString(), "[foobar:30.0, urls:12.0, foo:8.0, bar:7.235, more:6.0, some:3.0]");
    }
}
