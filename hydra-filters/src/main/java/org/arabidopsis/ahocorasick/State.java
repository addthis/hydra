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

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * A state represents an element in the Aho-Corasick tree.
 */
class State implements Serializable {

	private static final long serialVersionUID = -5773620235137320488L;
	
	private int depth;
	private EdgeList edgeList;
	private State fail;
	private Set<Object> outputs;

	public State(int depth) {
		this.depth = depth;
		this.edgeList = new SparseEdgeList();
		this.fail = null;
		this.outputs = new HashSet<Object>();
	}

	public State extend(char c) {
		if (this.edgeList.get(c) != null)
			return this.edgeList.get(c);
		State nextState = new State(this.depth + 1);
		this.edgeList.put(c, nextState);
		return nextState;
	}

	public State extendAll(char[] chars) {
		State state = this;
		for (int i = 0; i < chars.length; i++) {
			if (state.edgeList.get(chars[i]) != null)
				state = state.edgeList.get(chars[i]);
			else
				state = state.extend(chars[i]);
		}
		return state;
	}

	/**
	 * Returns the size of the tree rooted at this State. Note: do not call this
	 * if there are loops in the edgelist graph, such as those introduced by
	 * AhoCorasick.prepare().
	 */
	public int size() {
		char[] keys = edgeList.keys();
		int result = 1;
		for (int i = 0; i < keys.length; i++)
			result += edgeList.get(keys[i]).size();
		return result;
	}

	public State get(char c) {
		State s = this.edgeList.get(c);
		
		// The root state always returns itself when an state for the passed character doesn't exist
		if(s == null && isRoot()) {
			s = this;
		}
		
		return s;
	}

	public void put(char c, State s) {
		this.edgeList.put(c, s);
	}

	public char[] keys() {
		return this.edgeList.keys();
	}

	public State getFail() {
		return this.fail;
	}

	public void setFail(State f) {
		this.fail = f;
	}

	public void addOutput(Object o) {
		this.outputs.add(o);
	}

	public Set<Object> getOutputs() {
		return this.outputs;
	}
	
	public Boolean isRoot() {
		return depth == 0;
	}
	
	public int getDepth() {
		return depth;
	}
}
