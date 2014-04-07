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

package com.addthis.hydra.data.query.op.merge;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.hydra.data.query.op.MergedRow;

public interface MergedValue {

    public void merge(Bundle nextBundle, MergedRow mergedRow);

    public void emit(MergedRow mergedRow);

    public boolean isKey();

    public BundleField getFrom();

    public BundleField getTo();

    public void setFrom(BundleField from);

    public void setTo(BundleField to);

}