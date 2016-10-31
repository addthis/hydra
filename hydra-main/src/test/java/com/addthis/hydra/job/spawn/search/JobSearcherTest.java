package com.addthis.hydra.job.spawn.search;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobConfigManager;
import com.addthis.hydra.job.JobParameter;
import com.addthis.hydra.job.entity.JobMacro;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobSearcherTest {

    private ObjectMapper objectMapper = new ObjectMapper();

    private JobSearcher jobSearcher;
    private ByteArrayOutputStream outputStream;
    private JobConfigManager jobConfigManager;
    private Map<String, List<String>> aliases;
    private Map<String, JobMacro> macros;
    private Map<String, Job> jobs;

    @Before
    public void setup() throws IOException {
        jobConfigManager = mock(JobConfigManager.class);
        outputStream = new ByteArrayOutputStream();
        aliases = new HashMap<>();
        macros = new HashMap<>();
        jobs = new HashMap<>();
    }

    @Test
    public void notFound() throws IOException {
        addJob("JobSearchTest_notFound.jobconf");
        JsonNode result = doSearch();
        assertEquals("no macro match", 0, result.path("macros").size());
        assertEquals("no job match", 0, result.get("jobs").size());
    }

    @Test
    public void foundInJobConfig() throws IOException {
        addJob("JobSearchTest_foundInJobConfig.jobconf");
        JsonNode result = doSearch();
        assertEquals("no macro match", 0, result.path("macros").size());
        assertEquals("one job match", 1, result.get("jobs").size());
    }

    /**
     * Job -> Alias -> Search Pattern.
     * <p/>
     * Job config includes an alias whose value matches the search pattern
     */
    @Test
    public void foundInAlias() throws IOException {
        addJob("JobSearchTest_foundInAlias.jobconf");
        addAlias("JobSearchTest_foundInAlias", "something");
        JsonNode result = doSearch();
        assertEquals("no macro match", 0, result.path("macros").size());
        assertEquals("one job match", 1, result.get("jobs").size());
    }

    /**
     * Job -(param)-> Alias -> Search Pattern.
     * <p/>
     * Job parameter values is an alais; Alias value contains the search pattern
     */
    @Test
    public void foundInAlias_paramValueIsAlias() throws IOException {
        Job job = addJob("JobSearchTest_foundInAlias_paramValueIsAlias.jobconf");
        job.setParameters(Lists.newArrayList(new JobParameter("param", "%{JobSearchTest_foundInAlias_paramValueIsAlias}%", "")));
        addAlias("JobSearchTest_foundInAlias_paramValueIsAlias", "something");
        JsonNode result = doSearch();
        assertEquals("no macro match", 0, result.path("macros").size());
        assertEquals("one job match", 1, result.get("jobs").size());
        assertEquals("one block match", 1, result.get("jobs").get(0).get("results").size());
        assertEquals("one text location match", 1, result.get("jobs").get(0).get("results").get(0).get("matches").size());
        JsonNode matchLocation = result.get("jobs").get(0).get("results").get(0).get("matches").get(0);
        assertEquals("match line #", 3, matchLocation.get("lineNum").asInt());
        assertEquals("match start char #", 10, matchLocation.get("startChar").asInt());
        assertEquals("match end char #", 19, matchLocation.get("endChar").asInt());
    }

    /**
     * Job -> Macro -> Search Pattern.
     */
    @Test
    public void foundInMacro() throws IOException {
        addJob("JobSearchTest_foundInMacro.jobconf");
        addMacro("JobSearchTest_foundInMacro.macro");
        JsonNode result = doSearch();
        assertEquals("one macro match", 1, result.path("macros").size());
        assertEquals("one job match", 1, result.get("jobs").size());
    }

    /**
     * Job -> MacroA -> MacroB -> Search Pattern.
     */
    @Test
    public void foundInMacro_nestedInclusion() throws IOException {
        addJob("JobSearchTest_foundInMacro_nestedInclusion.jobconf");
        addMacro("JobSearchTest_foundInMacro_nestedInclusion.macro");
        addMacro("JobSearchTest_foundInMacro.macro");
        JsonNode result = doSearch();
        assertEquals("two macro matches", 2, result.path("macros").size());
        assertEquals("one job match", 1, result.get("jobs").size());
    }

    /**
     * Job -(param)-> Macro -> Search Pattern.
     *
     * Job references Macro via job parameter; Macro contains search pattern.
     */
    @Test
    public void foundInMacro_paramValueIsMacro() throws IOException {
        Job job = addJob("JobSearchTest_foundInMacro_paramValueIsMacro.jobconf");
        job.setParameters(Lists.newArrayList(new JobParameter("param", "%{JobSearchTest_foundInMacro.macro}%", "")));
        addMacro("JobSearchTest_foundInMacro.macro");
        JsonNode result = doSearch();
        assertEquals("one macro match", 1, result.path("macros").size());
        assertEquals("one job match", 1, result.get("jobs").size());
        assertEquals("one block match", 1, result.get("jobs").get(0).get("results").size());
        assertEquals("one text location match", 1, result.get("jobs").get(0).get("results").get(0).get("matches").size());
        JsonNode matchLocation = result.get("jobs").get(0).get("results").get(0).get("matches").get(0);
        assertEquals("match line #", 3, matchLocation.get("lineNum").asInt());
        assertEquals("match start char #", 10, matchLocation.get("startChar").asInt());
        assertEquals("match end char #", 19, matchLocation.get("endChar").asInt());
    }

    /**
     * Job -> MacroA -(param)-> MacroB -> Search Pattern.
     * <p>
     * Job references MacorA; MacroA references MacroB via job parameter; MacroB contains search pattern.
     */
    @Test
    @Ignore("Test will fail because the code does not support this user case")
    public void foundInMacro_paramValueIsMacro_nested() throws IOException {
        Job job = addJob("JobSearchTest_foundInMacro_paramValueIsMacro_nested.jobconf");
        job.setParameters(Lists.newArrayList(new JobParameter("param", "%{JobSearchTest_foundInMacro.macro}%", "")));
        addMacro("JobSearchTest_foundInMacro_paramValueIsMacro_nested.macro");
        addMacro("JobSearchTest_foundInMacro.macro");
        JsonNode result = doSearch();
        assertEquals("two macro matches", 2, result.path("macros").size());
        assertEquals("one job match", 1, result.get("jobs").size());
    }

    /**
     * Job -> default param value -> Search Pattern
     * <p/>
     * Search pattern is in the default value of a job parameter
     */
    @Test
    public void foundInParamInJobConfig_defaultParamValue() throws IOException {
        Job job = addJob("JobSearchTest_foundInParamInJobConfig_defaultParamValue.jobconf");
        job.setParameters(Lists.newArrayList(new JobParameter("param","","something")));
        JsonNode result = doSearch();
        assertEquals("one job match", 1, result.get("jobs").size());
        // There should be only one text match, not one from job config, another from job parameter
        assertEquals("one text match", 1, result.get("jobs").get(0).get("results").get(0).get("matches").size());
    }

    /**
     * Job -> assigned param value -> Search Pattern
     * <p/>
     * Search pattern is in the assigned value of a job parameter
     */
    @Test
    public void foundInParamInJobConfig_assignedParamValue() throws IOException {
        Job job = addJob("JobSearchTest_foundInParamInJobConfig_assignedParamValue.jobconf");
        job.setParameters(Lists.newArrayList(new JobParameter("param", "something", "")));
        JsonNode result = doSearch();
        assertEquals("one job match", 1, result.get("jobs").size());
        assertEquals("one text match", 1, result.get("jobs").get(0).get("results").get(0).get("matches").size());
    }

    /**
     * Job -> Macro -> default param value -> Search Pattern
     * <p/>
     * Search pattern is in the default value of a job parameter defined in a macro
     */
    @Test
    public void foundInParamInMacro_defaultParamValue() throws IOException {
        Job job = addJob("JobSearchTest_foundInParamInMacro_defaultParamValue.jobconf");
        addMacro("JobSearchTest_foundInParamInMacro_defaultParamValue.macro");
        job.setParameters(Lists.newArrayList(new JobParameter("param", "", "something")));
        JsonNode result = doSearch();
        assertEquals("one macro match", 1, result.get("macros").size());
        assertEquals("one job match", 1, result.get("jobs").size());
    }

    /**
     * Job -> Macro -> default param value -> Search Pattern
     * <p/>
     * Search pattern is in the assigned value of a job parameter defined in a macro
     */
    @Test
    public void foundInParamInMacro_assignedParamValue() throws IOException {
        Job job = addJob("JobSearchTest_foundInParamInMacro_assignedParamValue.jobconf");
        addMacro("JobSearchTest_foundInParamInMacro_assignedParamValue.macro");
        job.setParameters(Lists.newArrayList(new JobParameter("param", "something", "")));
        JsonNode result = doSearch();
        assertEquals("one macro match", 1, result.get("macros").size());
        assertEquals("one job match", 1, result.get("jobs").size());
    }

    /**
     * Job -> MacroA -> MacroB -> assigned param value -> Search Pattern
     * <p/>
     * MacroB contains a job parameter whose assigned value matches the search pattern.
     */
    @Test
    public void foundInParamInMacro_nestedInclusion() throws IOException {
        Job job = addJob("JobSearchTest_foundInParamInMacro_nestedInclusion.jobconf");
        addMacro("JobSearchTest_foundInParamInMacro_nestedInclusion.macro");
        addMacro("JobSearchTest_foundInParamInMacro_assignedParamValue.macro");
        job.setParameters(Lists.newArrayList(new JobParameter("param", "something", "")));
        JsonNode result = doSearch();
        assertEquals("one macro match", 2, result.get("macros").size());
        assertEquals("one job match", 1, result.get("jobs").size());
    }


    /**
     * When macro content contains the macro name, job search should not produce duplicate match locations.
     * <p/>
     * https://phabricator.clearspring.local/T66439
     */
    @Test
    public void dedupMatchLocations() throws IOException {
        addJob("JobSearchTest_dedupMatchLocations.jobconf");
        addMacro("JobSearchTest_dedupMatchLocations.macro");
        JsonNode result = doSearch("JobSearchTest_dedupMatchLocations.macro");
        assertEquals("one macro match", 1, result.path("macros").size());
        assertEquals("one job match", 1, result.get("jobs").size());
        assertEquals("one text location match", 1, result.get("jobs").get(0).get("results").get(0).get("matches").size());
    }

    private Job addJob(String testJobFilename) throws IOException {
        Job job = new Job(testJobFilename);
        job.setParameters(Lists.newArrayList());
        jobs.put(testJobFilename, job);
        String s = stringFromResource(testJobFilename);
        when(jobConfigManager.getConfig(testJobFilename)).thenReturn(s);
        return job;
    }

    private void addAlias(String name, String... values) {
        aliases.put(name, Lists.newArrayList(values));
    }

    private JobMacro addMacro(String testMacroName) throws IOException {
        JobMacro macro = new JobMacro("bob_owner", "dummy_group", testMacroName, stringFromResource(testMacroName));
        macros.put(testMacroName, macro);
        return macro;
    }

    private String stringFromResource(String resourceName) throws IOException {
        return Resources.toString(getClass().getResource(resourceName), Charsets.UTF_8);
    }

    private JsonNode doSearch() throws IOException {
        return doSearch("something");
    }

    private JsonNode doSearch(String search) throws IOException {
        JobSearcher jobSearcher = new JobSearcher(jobs, macros, aliases, jobConfigManager, new SearchOptions(search), outputStream);
        jobSearcher.run();
        return objectMapper.readTree(outputStream.toString());
    }

}
