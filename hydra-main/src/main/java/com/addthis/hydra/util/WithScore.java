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
package com.addthis.hydra.util;

/**
 * Something that needs to be stored with a score.
 *
 * @param <T> type of the element to be stored.
 */
public class WithScore<T> {
    public final T element;
    public final double score;

    public WithScore(T element, double score) {
        this.element = element;
        this.score = score;
    }

    public T getElement() {
        return element;
    }
}
