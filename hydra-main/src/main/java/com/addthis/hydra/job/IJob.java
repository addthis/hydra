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
package com.addthis.hydra.job;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import com.addthis.hydra.job.alert.JobAlert;
import com.addthis.hydra.job.entity.JobCommand;
import com.addthis.maljson.JSONObject;



public interface IJob extends Comparable<IJob> {

    public String getId();

    public String getOwner();

    public void setOwner(String owner);

    public String getCreator();

    public long getCreateTime();

    public String getDescription();

    public void setDescription(String description);

    public String getCommand();

    public void setCommand(String command);

    public String getKillSignal();

    public void setKillSignal(String killSignal);

    public int getPriority();

    public void setPriority(int priority);

    public boolean getStomp();

    public void setStomp(boolean stomp);

    public Long getSubmitTime();

    public void setSubmitTime(long submitTime);

    public Long getStartTime();

    public void setStartTime(Long startTime);

    public Long getEndTime();

    public void setEndTime(Long endTime);

    public Long getRekickTimeout();

    public void setRekickTimeout(Long rekick);

    public Long getMaxRunTime();

    public void setMaxRunTime(Long maxRunTime);

    public boolean isEnabled();

    public boolean setEnabled(boolean enabled);

    public Collection<JobParameter> getParameters();

    public void setParameters(Collection<JobParameter> parameters);

    public String getConfig();

    public void setConfig(String config);

    public String getOnCompleteURL();

    public void setOnCompleteURL(String url);

    public String getOnErrorURL();

    public void setOnErrorURL(String url);

    /* timeout in seconds */
    public int getOnCompleteTimeout();

    public void setOnCompleteTimeout(int timeout);

    public int getOnErrorTimeout();

    public void setOnErrorTimeout(int timeout);

    public int getBackups();

    public int getHourlyBackups();

    public int getDailyBackups();

    public int getWeeklyBackups();

    public int getMonthlyBackups();

    public void setHourlyBackups(int hourlyBackups);

    public void setDailyBackups(int dailyBackups);

    public void setWeeklyBackups(int weeklyBackups);

    public void setMonthlyBackups(int weeklyBackups);

    public void setBackups(int backups);

    public int getReplicas();

    public void setReplicas(int replicas);

    public int getReadOnlyReplicas();

    public void setReadOnlyReplicas(int readOnlyReplicas);

    public int getRunCount();

    public int incrementRunCount();

    public long getRunTime();

    public JobState getState();

    public boolean setState(JobState state);

    public void setAlerts(List<JobAlert> alerts);

    public List<JobAlert> getAlerts();

    public JobTask getTask(int id);

    public List<JobTask> getCopyOfTasks();

    public void addTask(JobTask task);

    public void setTasks(List<JobTask> tasks);

    public JobQueryConfig getQueryConfig();

    public void setQueryConfig(JobQueryConfig queryConfig);

    public JobCommand getSubmitCommand();

    public void setSubmitCommand(JobCommand submitCommand);

    public boolean getStrictReplicas();

    public void setStrictReplicas(boolean strictReplicas);

    public JSONObject toJSON() throws Exception;

    public HashMap<String, String> getProperties();

    public void setProperties(HashMap<String, String> properties);

    public int getReplicationFactor();

    public void setReplicationFactor(int replicationFactor);

    public boolean getDontAutoBalanceMe();

    public void setDontAutoBalanceMe(boolean dontAutoBalanceMe);

    public boolean getDontDeleteMe();

    public void setDontDeleteMe(boolean dontDeleteMe);

    public boolean getWasStopped();

    public void setWasStopped(boolean wasStopped);

    public int getMaxSimulRunning();

    public void setMaxSimulRunning(int maxSimulRunning);

    public String getMinionType();

    public void setMinionType(String minionType);

    public int getRetries();

    public void setRetries(int retries);
}
