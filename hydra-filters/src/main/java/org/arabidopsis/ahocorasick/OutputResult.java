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

public class OutputResult {
	/**
	 * The payload associated with the located substring.
	 */
	private Object output;

	/**
	 * The index (included) in the whole string where the located substring
	 * starts.
	 */
	private int startIndex;

	/**
	 * The index (excluded) in the whole string where the located substring
	 * ends.
	 */
	private int lastIndex;

	/**
	 * It creates a new OutputResult with the output and the last index passed
	 * as parameter. It set the startIndex attribute only if the output object
	 * is a String.
	 */
	public OutputResult(Object output, int startIndex, int lastIndex) {
		this.output = output;
		this.startIndex = startIndex;
		this.lastIndex = lastIndex;
	}

	public Object getOutput() {
		return output;
	}

	public int getStartIndex() {
		return startIndex;
	}

	public int getLastIndex() {
		return lastIndex;
	}

	/**
	 * An output with span <code>(start1,last1)</code> overlaps an output with
	 * span <code>(start2,last2)</code> if and only if either end points of the
	 * second output lie within the first chunk:
	 * <ul>
	 * <li> <code>start1 <= start2 < last1</code>, or
	 * <li> <code>start1 < last2 <= last1</code>.
	 * </ul>
	 */
	public boolean isOverlapped(OutputResult other) {
		return (this.startIndex <= other.startIndex && other.startIndex < this.lastIndex)
				|| (this.startIndex < other.lastIndex && other.lastIndex <= this.lastIndex);
	}

	/**
	 * An output <code>output1=(start1,last1)</code> dominates another output
	 * <code>output2=(start2,last2)</code> if and only if the outputs overlap
	 * and:
	 * 
	 * <ul>
	 * <li> <code>start1 &lt; start2</code> (leftmost), or
	 * <li> <code>start1 == start2</code> and <code>last1 &gt; last2</code>
	 * (longest).
	 * </ul>
	 */
	public boolean dominate(OutputResult other) {
		return isOverlapped(other)
				&& ((this.startIndex < other.startIndex) || (this.startIndex == other.startIndex && this.lastIndex > other.lastIndex));
	}
	
	public String toString() {
		return "[" + getStartIndex() + "," + getLastIndex() + "]: " + getOutput(); 
	}
}
