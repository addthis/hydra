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
package com.addthis.hydra.job.spawn;

import java.util.concurrent.ConcurrentMap;

import com.addthis.basis.collect.ConcurrentHashMapV8;

import com.addthis.codec.codables.Codable;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.util.DirectedGraph;

public class SpawnState implements Codable {

    final ConcurrentMap<String, Job> jobs = new ConcurrentHashMapV8<>();
    final DirectedGraph<String> jobDependencies = new DirectedGraph<>();
    SpawnBalancerConfig balancerConfig = new SpawnBalancerConfig();
}
