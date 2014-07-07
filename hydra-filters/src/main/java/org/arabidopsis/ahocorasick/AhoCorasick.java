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
import java.io.StringReader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.Version;

/**
 * <p>
 * An implementation of the Aho-Corasick string searching automaton. This
 * implementation of the <a
 * href="http://portal.acm.org/citation.cfm?id=360855&dl=ACM&coll=GUIDE"
 * target="_blank">Aho-Corasick</a> algorithm is optimized to work with chars.
 * </p>
 * 
 * <p>
 * Example usage: <code><pre>
       AhoCorasick tree = new AhoCorasick();
       tree.add("hello");
       tree.add("world");
       tree.prepare();

       Iterator searcher = tree.progressiveSearch("hello world");
       while (searcher.hasNext()) {
           SearchResult result = searcher.next();
           System.out.println(result.getOutputs());
           System.out.println("Found at index: " + result.getLastIndex());
       }
   </pre></code>
 * </p>
 */
public class AhoCorasick implements Serializable {

	private static final long serialVersionUID = -4152244297395236698L;

	private State root;
	private boolean prepared;
	private OutputSizeCalculator outputSizeCalculator;

	/**
	 * Default constructor, assuming that the outputs will be String.
	 */
	public AhoCorasick() {
		this.root = new State(0);
		this.prepared = false;
		this.outputSizeCalculator = new StringOutputSizeCalculator();
	}

	/**
	 * This constructor allows to set an specific object to calculate the size
	 * of the outputs.
	 */
	public AhoCorasick(OutputSizeCalculator outputSizeCalculator) {
		this();

		if (outputSizeCalculator == null) {
			throw new IllegalStateException(
					"The outputSizeCalculator attribute musn't be null");
		}

		this.outputSizeCalculator = outputSizeCalculator;
	}

	/**
	 * @see AhoCorasick#add(char[], Object)
	 */
	public void add(String keyword, Object output) {
		add(keyword.toCharArray(), output);
	}

	/**
	 * @see AhoCorasick#add(char[])
	 */
	public void add(String keyword) {
		add(keyword.toCharArray());
	}

	/**
	 * Adds a new keyword with the associated String as output. During search,
	 * if the keyword is matched, output will be one of the yielded elements in
	 * SearchResults.getOutputs().
	 * 
	 * @see AhoCorasick#add(char[], Object)
	 */
	public void add(char[] keyword) {
		add(keyword, new String(keyword));
	}

	/**
	 * Adds a new keyword with the given output. During search, if the keyword
	 * is matched, output will be one of the yielded elements in
	 * SearchResults.getOutputs().
	 */
	public void add(char[] keyword, Object output) {
		if (this.prepared)
			throw new IllegalStateException(
					"can't add keywords after prepare() is called");
		State lastState = this.root.extendAll(keyword);
		lastState.addOutput(output);
	}

	/**
	 * Prepares the automaton for searching. This must be called before any
	 * searching().
	 */
	public void prepare() {
		this.prepareFailTransitions();
		this.prepared = true;
	}

	/**
	 * @see AhoCorasick#progressiveSearch(char[])
	 */
	public Iterator<SearchResult> progressiveSearch(String inputText) {
		return progressiveSearch(inputText.toCharArray());
	}

	/**
	 * Starts a new search, and returns an Iterator of SearchResults.
	 */
	public Iterator<SearchResult> progressiveSearch(char[] chars) {
		return new Searcher(this, this.startSearch(chars));
	}

	/**
	 * @see AhoCorasick#completeSearch(char[])
	 */
	public List<OutputResult> completeSearch(String inputText,
			boolean allowOverlapping, boolean onlyTokens) {
		return completeSearch(inputText.toCharArray(), allowOverlapping,
				onlyTokens);
	}

	/**
	 * It make a whole new search, and it returns all the OutputResult objects,
	 * ordered by the startIndex attribute. If the parameter allowOverlapping is
	 * false, the overlapped outputs will be removed.
	 * 
	 * If the flag 'onlyTokens' is true only outputs that are tokens are
	 * returned.
	 * 
	 * @see AhoCorasick#removeOverlapping(List)
	 */
	public List<OutputResult> completeSearch(char[] chars,
			boolean allowOverlapping, boolean onlyTokens) {
		List<OutputResult> result;
		Searcher searcher = new Searcher(this, this.startSearch(chars));

		// Recollection the valid outputs
		result = recollectOutputResults(searcher, chars, onlyTokens);

		// Sorting the result list according to the startIndex of each output
		sortOutputResults(result);

		// Removing overlappings
		if (!allowOverlapping) {
			removeOverlapping(result);
		}

		return result;
	}

	/**
	 * The non-overlapping outputs are taken to be the left-most and
	 * longest-matching, according to the following definitions. An output with
	 * span <code>(start1,last1)</code> overlaps an output with span
	 * <code>(start2,last2)</code> if and only if either end points of the
	 * second output lie within the first chunk:
	 * <ul>
	 * <li> <code>start1 <= start2 < last1</code>, or
	 * <li> <code>start1 < last2 <= last1</code>.
	 * </ul>
	 * 
	 * For instance, <code>(0,1)</code> and <code>(1,3)</code> do not overlap,
	 * but <code>(0,1)</code> overlaps <code>(0,2)</code>, <code>(1,2)</code>
	 * overlaps <code>(0,2)</code>, and <code>(1,7)</code> overlaps
	 * <code>(2,3)</code>.
	 * 
	 * <p>
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
	void removeOverlapping(List<OutputResult> outputResults) {
		int currentIndex = 0;
		OutputResult current, next;

		while (currentIndex < (outputResults.size() - 1)) {
			current = outputResults.get(currentIndex);
			// We will check the current output with the next one
			next = outputResults.get(currentIndex + 1);

			if (!current.isOverlapped(next)) {
				// without overlapping we can advance without problems
				currentIndex++;
			} else if (current.dominate(next)) {
				// the current one dominates the next one -> we remove the next
				// one
				outputResults.remove(currentIndex + 1);
			} else {
				// the next one dominates the current one -> we remove the
				// current one
				outputResults.remove(currentIndex);
			}
		}
	}

	/**
	 * DANGER DANGER: dense algorithm code ahead. Very order dependent.
	 * Initializes the fail transitions of all states except for the root.
	 */
	private void prepareFailTransitions() {
		Queue q = new Queue();
		char[] keys = this.root.keys();
		State state, r, s;

		for (char key : keys) {
			state = this.root.get(key);
			state.setFail(this.root);
			q.add(state);
		}

		while (!q.isEmpty()) {
			state = q.pop();
			keys = state.keys();
			for (char key : keys) {
				s = state.get(key);
				q.add(s);
				r = state.getFail();
				while (r.get(key) == null)
					r = r.getFail();
				s.setFail(r.get(key));
				s.getOutputs().addAll(r.get(key).getOutputs());
			}
		}
	}

	/**
	 * Returns the root of the tree. Package protected, since the user probably
	 * shouldn't touch this.
	 */
	State getRoot() {
		return this.root;
	}

	/**
	 * Begins a new search using the raw interface. Package protected.
	 */
	SearchResult startSearch(char[] chars) {
		if (!this.prepared) {
			throw new IllegalStateException(
					"can't start search until prepare()");
		}

		return continueSearch(new SearchResult(this.root, chars, 0));
	}

	/**
	 * Continues the search, given the initial state described by the
	 * lastResult. Package protected.
	 */
	SearchResult continueSearch(SearchResult lastResult) {
		char currentChar;
		SearchResult searchResult = null;
		char[] chars = lastResult.chars;
		State state = lastResult.lastMatchedState;
		Integer currentIndex = lastResult.lastIndex;

		while (shouldContinueSearching(searchResult, currentIndex, chars)) {
			currentChar = chars[currentIndex];

			// Locating the right state
			while (state.get(currentChar) == null)
				state = state.getFail();
			state = state.get(currentChar);

			if (shouldCreateSearchResult(state)) {
				// We have reached a node with outputs -> a new result
				searchResult = new SearchResult(state, chars, currentIndex + 1);
			} else {
				currentIndex++; // We should advance to the next node
			}
		}
		return searchResult;
	}

	// This method returns true if a new SearchResult should be created in the
	// state set by the params
	private boolean shouldCreateSearchResult(State state) {
		boolean result = false;

		if (state.getOutputs().size() > 0) {
			result = true;
		}

		return result;
	}

	private boolean shouldContinueSearching(SearchResult searchResult,
			Integer currentIndex, char[] inputText) {
		return searchResult == null && currentIndex != null
				&& currentIndex < inputText.length;
	}

	private TokensInformation extractTokensInformation(char[] chars) {
		TokensInformation result = new TokensInformation();
		List<Integer> starts = new ArrayList<Integer>();
		List<Integer> ends = new ArrayList<Integer>();
		Tokenizer tokenizer = createTokenizer(chars);
		OffsetAttribute offsetAttribute = tokenizer
				.addAttribute(OffsetAttribute.class);

		try {
			while (tokenizer.incrementToken()) {
				starts.add(offsetAttribute.startOffset());
				ends.add(offsetAttribute.endOffset());
			}
			tokenizer.end();
			tokenizer.close();
		} catch (Exception e) {
			System.err.println("Error tokenizing the input text: "
					+ e.getMessage());
		}

		result.setEnds(ends);
		result.setStarts(starts);

		return result;
	}

	private Tokenizer createTokenizer(char[] chars) {
		return new StandardTokenizer(Version.LUCENE_36, new StringReader(
				new String(chars)));
	}

	private void sortOutputResults(List<OutputResult> outputResults) {
		Collections.sort(outputResults, new Comparator<OutputResult>() {
			@Override
			public int compare(OutputResult o1, OutputResult o2) {
				return o1.getStartIndex() - o2.getStartIndex();
			}
		});
	}

	private List<OutputResult> recollectOutputResults(Searcher searcher,
			char[] chars, boolean onlyTokens) {
		Integer startIndex;
		SearchResult searchResult;
		TokensInformation tokensInformation = null;
		List<OutputResult> result = new ArrayList<OutputResult>();

		if (onlyTokens) {
			tokensInformation = extractTokensInformation(chars);
		}

		// Iteration over the results
		while (searcher.hasNext()) {
			searchResult = searcher.next();

			// Iterating over the outputs
			for (Object output : searchResult.getOutputs()) {
				startIndex = searchResult.lastIndex
						- outputSizeCalculator.calculateSize(output);

				if (!onlyTokens
						|| tokensInformation.areValidOffsets(startIndex,
								searchResult.lastIndex)) {
					result.add(new OutputResult(output, startIndex,
							searchResult.lastIndex));
				}
			}
		}

		return result;
	}
}
