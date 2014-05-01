/**
 * Copyright (c) 2005, 2008 Danny Yoo (http://bitbucket.org/jlanchas/aho-corasick/)
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *
 * Neither the name of the Carnegie Institution of Washington nor
 * the names of its contributors may be used to endorse or promote
 * products derived from this software without specific prior written
 * permission
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 **/
package org.arabidopsis.ahocorasick;

import java.util.Collections;
import java.util.List;

public class TokensInformation {

	// The start offsets of the tokens
	List<Integer> starts;

	// The end offsets of the tokens
	List<Integer> ends;

	// This method sets the starts attribute and it sorts it
	public void setStarts(List<Integer> starts) {
		this.starts = starts;
		Collections.sort(this.starts);
	}

	// This method sets the ends attribute and it sorts it
	public void setEnds(List<Integer> ends) {
		this.ends = ends;
		Collections.sort(this.ends);
	}

	/**
	 * This method returns true if the 'start' param is in the 'starts'
	 * attribute and the 'end' param is in the 'ends' attribute.
	 */
	public boolean areValidOffsets(Integer start, Integer end) {
		return starts != null && ends != null
				&& Collections.binarySearch(starts, start) >= 0
				&& Collections.binarySearch(ends, end) >= 0;
	}
}
