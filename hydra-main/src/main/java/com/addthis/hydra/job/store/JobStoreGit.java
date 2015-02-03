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
package com.addthis.hydra.job.store;


import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import java.util.Arrays;
import java.util.List;

import com.addthis.basis.util.Files;
import com.addthis.basis.util.Parameter;

import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONException;
import com.addthis.maljson.JSONObject;

import com.google.common.collect.Iterables;

import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.DiffCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ListBranchCommand;
import org.eclipse.jgit.api.ResetCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileBasedConfig;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.eclipse.jgit.treewalk.AbstractTreeIterator;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.eclipse.jgit.treewalk.filter.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for storing/retrieving files from a git repository.
 */
public class JobStoreGit {

    private final File baseDir;
    private final File gitDir;
    private final File jobDir;
    private final FileRepository repository;
    private final Git git;
    private final boolean remote = Parameter.boolValue("job.store.remote", false);
    private static final String branchName = Parameter.value("cluster.name", "localhost");
    private static final String gitUser = Parameter.value("git.user", "anonymous");
    private static final String gitPassword = Parameter.value("git.password", "");
    private static final String gitUrl = Parameter.value("git.url", "");
    private static final UsernamePasswordCredentialsProvider provider = new UsernamePasswordCredentialsProvider(gitUser, gitPassword);
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(JobStoreGit.class);
    boolean haveRemoteBranch = false;

    /**
     * Set up a git repository in a directory.
     *
     * @param baseDir The directory that will store the files.
     * @throws IOException If there is a problem writing to the directory
     */
    public JobStoreGit(File baseDir) throws Exception {
        this.baseDir = baseDir;
        this.gitDir = new File(baseDir, ".git");
        this.jobDir = new File(baseDir, "jobs");
        repository = new FileRepository(gitDir);
        git = new Git(repository);
        initialize();
    }

    /**
     * Fetch the version of a config after a particular commit in its history
     *
     * @param jobId    The job id to check
     * @param commitId The commit id to fetch
     * @return The contents of the file after the specified time
     * @throws GitAPIException If there is a problem retrieving the file after the specified commit
     * @throws IOException     If there is a problem writing to the file
     */
    public String fetchJobConfigFromHistory(String jobId, String commitId) throws GitAPIException, IOException {
        synchronized (gitDir) {
            File file = getFileForJobId(jobId);
            git.checkout().addPath(getPathForJobId(jobId)).setStartPoint(commitId).setName("master").call();
            String rv = new String(Files.read(file));
            git.reset().setMode(ResetCommand.ResetType.HARD).call();
            return rv;
        }
    }

    /**
     * Returns the hash of the last commit before a job was deleted.
     * 
     * This method uses the following git command equivalent:
     * <pre>
     * git log --all --skip=1 -n 1 -- jobs/[jobId]
     * </pre>
     * which only works as intended with deleted jobs. 
     * 
     * @param jobId     The id of a deleted job.
     * @throws GitAPIException
     * @throws IOException
     */
    public String getCommitHashBeforeJobDeletion(String jobId) throws GitAPIException, IOException {
        // skip=1 skips the deletion, and -n 1 returns only the commit immediately before that
        Iterable<RevCommit> iter = git.log().all().addPath(getPathForJobId(jobId)).setSkip(1).setMaxCount(1).call();
        RevCommit commit = Iterables.getFirst(iter, null);
        return commit == null ? null : commit.getName();
    }

    /**
     * Get a summary of the diff, mainly used to automatically generate useful commit messages
     *
     * @param jobId The job to summarize the diff for
     * @return A string description of how the file has changed, e.g. "(added 3 lines, removed 1)"
     * @throws IOException     If there is a problem reading from the file
     * @throws GitAPIException If there is a problem getting the diff from git
     */
    public String getDiffSummary(String jobId) throws IOException, GitAPIException {
        // Have to start each count at -1 to account for the line that looks like ---a/filename
        int added = -1;
        int removed = -1;
        String diff = getDiff(jobId, null);
        for (String line : diff.split("\n")) {
            if (line.startsWith("-")) {
                removed++;
            } else if (line.startsWith("+")) {
                added++;
            }
        }
        if (added <= 0 && removed <= 0) {
            return "(no changes)";
        } else {
            return "(added " + added + " line" + (added == 1 ? "" : "s") + ", removed " + removed + ")";
        }

    }

    /**
     * Get the diff of a file against a particular commit
     *
     * @param jobId    The job config to diff
     * @param commitId If specified, the commit to compare against; otherwise, compare against the latest revision
     * @return A string description of the diff, comparable to the output of "git diff filename"
     * @throws GitAPIException If there is a problem fetching the diff from git
     * @throws IOException     If there is a problem reading from the file
     */
    public String getDiff(String jobId, String commitId) throws GitAPIException, IOException {
        OutputStream out = new ByteArrayOutputStream();
        DiffCommand diff = git.diff()
                .setPathFilter(PathFilter.create(getPathForJobId(jobId)))
                .setOutputStream(out)
                .setSourcePrefix("old:")
                .setDestinationPrefix("new:");
        if (commitId != null) {
            diff.setOldTree(getTreeIterator(commitId));
            diff.setNewTree(getTreeIterator("HEAD"));
        }
        diff.call();
        return out.toString();
    }

    /*
     * Internal function for generating an automatic commit message based on who changed the file and how it changed
     */
    private String generateFinalCommitMessage(String jobId, String author, String commitMessage) throws GitAPIException, IOException {
        return (author != null ? author : "unknown") + ": " + (commitMessage != null && !commitMessage.isEmpty() ? commitMessage : "[AUTO]") + " " + getDiffSummary(jobId);
    }

    /**
     * Get a JSON representation of the log of changes to a file
     *
     * @param jobId The jobId to fetch the log for
     * @return A log of the form [{commit:commitid, time:time, msg:commitmessage}, ...]
     * @throws GitAPIException If there is a problem fetching the log
     * @throws JSONException   If there is a problem generating the JSON
     */
    public JSONArray getGitLog(String jobId) throws Exception {
        JSONArray rv = new JSONArray();
        for (RevCommit commit : git.log().addPath(getPathForJobId(jobId)).call()) {
            JSONObject commitJson = new JSONObject().put("commit", commit.getName()).put("time", 1000L * (commit.getCommitTime())).put("msg", commit.getFullMessage());
            rv.put(commitJson);
        }
        return rv;
    }

    /**
     * Write a job config to a file and commit it
     *
     * @param jobId         The jobId being updated
     * @param author        The author who made the most recent change
     * @param config        The latest version of the config
     * @param commitMessage The base commit message to use, or null to use an automatic one
     * @throws GitAPIException If there is a problem talking to the git repo
     * @throws IOException     If there is a problem writing to the file system
     */
    public void commit(String jobId, String author, String config, String commitMessage) throws GitAPIException, IOException {
        synchronized (gitDir) {
            File file = getFileForJobId(jobId);
            assert (file.exists() || file.createNewFile());
            Files.write(file, config.getBytes(), false);
            commitMessage = generateFinalCommitMessage(jobId, author, commitMessage);
            git.add().addFilepattern(getPathForJobId(jobId)).call();
            git.commit().setMessage(commitMessage).call();
            push();
        }

    }

    /**
     * Pull-rebase and push to the remote repo if git.remote is enabled
     *
     * @throws GitAPIException If there is a failure talking to the remote repository
     */
    public void push() throws GitAPIException {
        if (remote) {
            synchronized (gitDir) {
                if (haveRemoteBranch()) {
                    git.pull().setCredentialsProvider(provider).setRebase(true).call();
                }
                git.push().setCredentialsProvider(provider).setRemote("origin").call();
            }
        }
    }

    /**
     * Initialize the git repo in the specified directory
     *
     * @throws Exception If there is a problem during initialization
     */
    private void initialize() throws Exception {
        synchronized (gitDir) {
            FileBasedConfig config = repository.getConfig();
            for (String branch : Arrays.asList(branchName, "master")) {
                config.setString("branch", branch, "merge", "refs/heads/" + branch);
                config.setString("branch", branch, "remote", "origin");

            }
            config.setString("remote", "origin", "fetch", "+refs/*:refs/*");
            config.setString("remote", "origin", "url", gitUrl);
            if (!gitDir.exists()) {
                if (remote) {
                    new CloneCommand().setCredentialsProvider(provider).setRemote("origin").setBranch("master").setURI(gitUrl).setDirectory(baseDir).call();
                    git.branchCreate().setName(branchName).call();
                    git.checkout().setName(branchName).call();
                } else {
                    repository.create();
                }
            }
            if (!jobDir.exists()) {
                Files.initDirectory(jobDir);
            }
        }
    }

    /*
     * Internal function for fetching the TreeIterator object corresponding to the git state after a particular commit
     */
    private AbstractTreeIterator getTreeIterator(String name) throws IOException {
        final ObjectId id = repository.resolve(name);
        if (id == null) {
            throw new IllegalArgumentException(name);
        }
        final CanonicalTreeParser p = new CanonicalTreeParser();
        final ObjectReader or = repository.newObjectReader();
        try {
            p.reset(or, new RevWalk(repository).parseTree(id));
            return p;
        } finally {
            or.release();
        }
    }

    /*
     * Internal function for generating an automatic message for deleting a config
     */
    private String getDeleteCommitMessage(String jobId) {
        return "Deleted " + jobId;
    }

    /**
     * Delete a config and remove it from git tracking
     *
     * @param jobId The job config to remove
     */
    public void remove(String jobId) {
        File file = getFileForJobId(jobId);
        synchronized (gitDir) {
            try {
                git.rm().addFilepattern(getPathForJobId(jobId)).call();
                git.commit().setMessage(getDeleteCommitMessage(jobId)).call();
                push();
            } catch (Exception e) {
                // Maybe the file hadn't been committed yet, or was not tracked in the first place
                assert (file.delete());
            }
        }
    }

    /**
     * Internal function to fetch the file corresponding to a job id
     *
     * @param jobId The job id to look for
     * @return The corresponding file
     */
    private File getFileForJobId(String jobId) {
        return new File(jobDir, jobId);
    }

    /**
     * Internal function to fetch the particular relative file path for a jobId that jgit needs to work right
     *
     * @param jobId The jobId to look for
     * @return The corresponding path
     */
    private String getPathForJobId(String jobId) {
        return jobDir.getName() + "/" + jobId;
    }

    private boolean haveRemoteBranch() throws GitAPIException {
        if (haveRemoteBranch) {
            return true; // If found once, the branch should stick around under any reasonable circumstances
        }
        List<Ref> refs = git.branchList().setListMode(ListBranchCommand.ListMode.REMOTE).call();
        for (Ref ref : refs) {
            String name = ref.getName();
            if (("/refs/remotes/origin/" + branchName).equals(name)) {
                haveRemoteBranch = true;
                return true;
            }
        }
        return false;
    }
}
