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
/**
 * <p>A job is a unit of work that is submitted to the Hydra cluster.</p>
 *
 * <p>Each job is defined by its command, the configuration you provide, number
 * of shards, how long it should run, etc. Jobs are broken apart into a number
 * of tasks. These tasks are then allocated among the minions. A minion may end
 * with multiple tasks from the same job (if that is best for cluster health as
 * a whole, or if that job has more tasks than there are total minions) but
 * usually they are spread around.</p>
 *
 * <p>Jobs can in principle be arbitrary programs. In practice most jobs ingest
 * streams of data that is transformed into bundles, applies some filters to
 * those bundles and emits an output.</p>
 *
 * <p>The most common Hydra jobs are of two varieties:</p>
 * <p><ul><li>Split jobs take in lines of data (think log files) and emit new
 * lines. It might change the format (text in, binary out), drop lines that
 * fail to some predicate, create multiple derived lines from each input, make
 * all strings lowercase, or other arbitrary transformations. But it's always
 * lines in, lines out.</li>
 * <li>TreeBuilder (or Map) jobs take in log lines of input (such as those
 * emitted by a split job) and build a tree representation of the data. This
 * hierarchical databases can then be explored through the distribute Hydra
 * query system.</li></ul></p>
 * <p>Split jobs and TreeBuilder jobs are both specified using the
 * {@link com.addthis.hydra.task.map.StreamMapper <code>type:"map"</code>} job type.
 * We suggest starting there to learn about Hydra job specification.</p>
 */
package com.addthis.hydra.task;