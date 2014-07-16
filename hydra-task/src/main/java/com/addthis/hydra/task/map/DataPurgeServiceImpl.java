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
package com.addthis.hydra.task.map;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.addthis.basis.util.Strings;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataPurgeServiceImpl implements DataPurgeService {

    private static final Logger logger = LoggerFactory.getLogger(DataPurgeServiceImpl.class);

    private static final String dirSeperator = File.separator;
    private static final String dirRegexSeperator = File.separator.equals("\\") ? "\\\\" : File.separator;

    public DataPurgeServiceImpl() {
    }

    @Override
    public boolean purgeData(DataPurgeConfig dataPurgeConfig, DateTime currentTime) {
        if (!validatePurgeParameters(dataPurgeConfig, currentTime)) {
            return false;
        }
        DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(dataPurgeConfig.getDatePathFormat());
        DateTime oldestDataAllowed;
        if (dataPurgeConfig.getMaxAgeInDays() > 0) {
            oldestDataAllowed = currentTime.plusDays(-dataPurgeConfig.getMaxAgeInDays());
        } else {
            oldestDataAllowed = currentTime.plusHours(-dataPurgeConfig.getMaxAgeInHours());
        }
        logger.debug("Oldest data allowed {} , current time is {}", new Object[]{oldestDataAllowed, currentTime});
        for (String directoryPrefix : dataPurgeConfig.getDirectoryPrefix()) {
            for (File prefixDirectory : expandPrefix(directoryPrefix)) {
                List<File> subdirectories = getSubdirectoryList(prefixDirectory, null);
                for (File subdirectory : subdirectories) {
                    logger.trace("Considering directory {} for purge", subdirectory);
                    safeDelete(prefixDirectory.getPath(), dateTimeFormatter, oldestDataAllowed, subdirectory, dataPurgeConfig.isFileBasedPurge(), dataPurgeConfig.getDateStartIndex(),
                            dataPurgeConfig.getDateStringLength());
                }
                if (dataPurgeConfig.getCleanEmptyParents()) {
                    for (File directory : subdirectories) {
                        if (directory.list() != null && directory.list().length == 0) {
                            try {
                                FileUtils.deleteDirectory(directory);
                            } catch (IOException e) {
                                logger.warn("Failed to delete empty directory {}", directory);
                            }
                        }
                    }
                }
            }
        }

        return true;

    }

    protected List<File> generateDirectoryList(String prefixDirectory) {
        List<File> directoryList = new LinkedList<File>();
        for (File directory : expandPrefix(prefixDirectory)) {
            logger.trace("prefix expanded {} to {}", prefixDirectory, directory);
            getSubdirectoryList(directory, directoryList);
        }
        return directoryList;
    }

    protected void safeDelete(String directoryPrefix, DateTimeFormatter dateTimeFormatter, DateTime oldestDataAllowed, File directory, boolean fileBasedPurge, int dateStartIndex, int dateStringLength) {
        String dateString;
        if (fileBasedPurge) {
            File[] fileList = directory.listFiles(new FileFilter() {
                @Override
                public boolean accept(File file) {
                    return file.isFile();
                }
            });
            if (fileList != null && fileList.length > 0) {
                for (File file : fileList) {
                    String fileName = file.getName();
                    dateString = fileName.substring(dateStartIndex, dateStringLength + dateStartIndex);
                    if (shouldDelete(dateTimeFormatter, oldestDataAllowed, dateString)) {
                        delete(file);
                    }
                }
            }
        } else {
            String directoryStr = directory.getPath().replace(directoryPrefix, "");
            if (directoryStr.startsWith(dirSeperator)) {
                directoryStr = directoryStr.substring(1);
            }
            if (shouldDelete(dateTimeFormatter, oldestDataAllowed, directoryStr)) {
                delete(directory);
            }
        }
    }

    private void delete(File file) {
        String name;
        try {
            name = file.getCanonicalPath();
        } catch (IOException e) {
            name = "(unk-path)" + file.getName();
        }
        logger.debug("Deleting: " + name);
        try {
            if (file.isDirectory()) {
                FileUtils.deleteDirectory(file);
            } else {
                FileUtils.deleteQuietly(file);
            }
        } catch (IOException e) {
            logger.error("error purging : " + file, e);
        }
    }

    protected boolean shouldDelete(DateTimeFormatter dateTimeFormatter, DateTime oldestDataAllowed, String dateString) {
        boolean result = false;
        DateTime time = null;
        try {
            time = dateTimeFormatter.parseDateTime(dateString);
        } catch (Exception e) {
            // ignore this directory
        }
        if (time != null && time.isBefore(oldestDataAllowed)) {
            result = true;
        }
        return result;
    }

    protected List<File> expandPrefix(String path) {
        if (path.indexOf('*') == -1) {
            LinkedList<File> list = new LinkedList<File>();
            list.add(new File(path));
            return list;
        }
        File cur = path.startsWith(dirSeperator) ? new File(dirSeperator) : new File(".");
        LinkedList<File> list = new LinkedList<File>();
        String[] tokens = Strings.splitArray(path, dirRegexSeperator);
        expandPrefix(list, cur, tokens, 0);
        return list;
    }

    protected void expandPrefix(List<File> list, File cur, String[] tokens, int index) {
        if (index == tokens.length) {
            if (cur.isDirectory() && cur.exists()) {
                list.add(cur);
            }
            return;
        }
        String tok = tokens[index];
        if (tok.indexOf('*') >= 0) {
            FileFilter fileFilter = new WildcardFileFilter(tok);
            File[] find = cur.listFiles(fileFilter);

            if (find != null) {
                for (File found : find) {
                    if (found.isDirectory()) {
                        expandPrefix(list, found, tokens, index + 1);
                    }
                }
            }
        } else {
            expandPrefix(list, new File(cur, tok), tokens, index + 1);
        }
    }

    /**
     * recursively add subdirectories into the directoryList
     */
    protected List<File> getSubdirectoryList(File current, List<File> directoryList) {
        if (directoryList == null) {
            directoryList = new ArrayList<File>();
        }
        directoryList.add(current);
        if (current.isDirectory()) {
            File[] fileArray = current.listFiles(new FileFilter() {
                @Override
                public boolean accept(File file) {
                    return file.isDirectory();
                }
            });
            if (fileArray != null) {
                for (File directory : fileArray) {
                    getSubdirectoryList(directory, directoryList);
                }
            }
        }
        return directoryList;
    }

    private boolean validatePurgeParameters(DataPurgeConfig dataPurgeConfig, DateTime currentTime) {
        if (dataPurgeConfig.getDirectoryPrefix() == null || dataPurgeConfig.getDirectoryPrefix().length == 0) {
            logger.error("Directory prefix can not be null or blank");
            return false;
        }
        if (dataPurgeConfig.getDatePathFormat() == null || dataPurgeConfig.getDatePathFormat().isEmpty()) {
            logger.error("Date path format can not be null or blank");
            return false;
        }
        if (currentTime == null) {
            logger.error("Current time can not be null");
            return false;
        }
        if (dataPurgeConfig.getMaxAgeInDays() <= 0 && dataPurgeConfig.getMaxAgeInHours() <= 0) {
            logger.error("max age must be > 0");
            return false;
        }
        if (dataPurgeConfig.isFileBasedPurge() && dataPurgeConfig.getDateStartIndex() < 0) {
            logger.error("File based purges require the dataStartIndex to be set");
            return false;
        }
        if (dataPurgeConfig.getDateStartIndex() >= 0 && dataPurgeConfig.getDateStringLength() < 0) {
            logger.error("Date start index was set but date string length was not defined");
            return false;
        }
        return true;
    }
}
