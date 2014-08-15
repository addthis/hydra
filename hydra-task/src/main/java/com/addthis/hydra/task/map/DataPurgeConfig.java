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

import com.addthis.codec.annotations.FieldConfig;

/**
 * @user-reference
 */
public class DataPurgeConfig {

    /**
     * Base directory to search for data to purge. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private String[] directoryPrefix;

    /**
     * The <a href="http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html">DateTimeFormat</a>
     * for the date substring. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private String datePathFormat;

    /**
     * Maximum age in days of data to retain. Anything
     * older than this value will be purged. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private int maxAgeInDays;

    /**
     * If maxAgeInDays is zero then this parameter is the
     * maximum age in hours of data to retain. Anything
     * older than this value will be purged.
     */
    @FieldConfig(codable = true)
    private int maxAgeInHours = -1;

    /**
     * If true then purge should be done by file rather than by directory. Default is false.
     */
    @FieldConfig(codable = true)
    private boolean fileBasedPurge = false;

    /**
     * If fileBasedPurge is true then use this as the start index
     * to look for the date string in the file. If performing
     * a file-based purge then this field is required.
     */
    @FieldConfig(codable = true)
    private int dateStartIndex = -1;

    /**
     * If fileBasedPurge is true then use this as the length
     * of the substring to look for the date string in the file.
     * If performing a file-based purge then this field is required.
     */
    @FieldConfig(codable = true)
    private int dateStringLength = -1;

    /**
     * If true then delete empty directories. Default is false.
     */
    @FieldConfig(codable = true)
    private boolean cleanEmptyParents = false;

    public DataPurgeConfig() {
    }

    public void setDirectoryPrefix(String[] directoryPrefix) {
        this.directoryPrefix = directoryPrefix;
    }

    public void setDatePathFormat(String datePathFormat) {
        this.datePathFormat = datePathFormat;
    }

    public void setMaxAgeInDays(int maxAgeInDays) {
        this.maxAgeInDays = maxAgeInDays;
    }

    public void setMaxAgeInHours(int maxAgeInHours) {
        this.maxAgeInHours = maxAgeInHours;
    }

    public void setFileBasedPurge(boolean fileBasedPurge) {
        this.fileBasedPurge = fileBasedPurge;
    }

    public void setDateStartIndex(int dateStartIndex) {
        this.dateStartIndex = dateStartIndex;
    }

    public void setDateStringLength(int dateStringLength) {
        this.dateStringLength = dateStringLength;
    }

    public void setCleanEmptyParents(boolean cleanEmptyParents) {
        this.cleanEmptyParents = cleanEmptyParents;
    }

    public String[] getDirectoryPrefix() {
        return directoryPrefix;
    }

    public String getDatePathFormat() {
        return datePathFormat;
    }

    public int getMaxAgeInDays() {
        return maxAgeInDays;
    }

    public int getMaxAgeInHours() {
        return maxAgeInHours;
    }

    public boolean isFileBasedPurge() {
        return fileBasedPurge;
    }

    public int getDateStartIndex() {
        return dateStartIndex;
    }

    public int getDateStringLength() {
        return dateStringLength;
    }

    public boolean getCleanEmptyParents() {
        return cleanEmptyParents;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DataPurgeConfig that = (DataPurgeConfig) o;

        if (dateStartIndex != that.dateStartIndex) {
            return false;
        }
        if (fileBasedPurge != that.fileBasedPurge) {
            return false;
        }
        if (maxAgeInDays != that.maxAgeInDays) {
            return false;
        }
        if (datePathFormat != null ? !datePathFormat.equals(that.datePathFormat) : that.datePathFormat != null) {
            return false;
        }
        if (directoryPrefix != null ? !directoryPrefix.equals(that.directoryPrefix) : that.directoryPrefix != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = directoryPrefix != null ? directoryPrefix.hashCode() : 0;
        result = 31 * result + (datePathFormat != null ? datePathFormat.hashCode() : 0);
        result = 31 * result + maxAgeInDays;
        result = 31 * result + (fileBasedPurge ? 1 : 0);
        result = 31 * result + dateStartIndex;
        return result;
    }
}
