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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import junit.framework.TestCase;

/**
 * Junit test cases for AhoCorasick.
 */
public class TestAhoCorasick extends TestCase {

	private AhoCorasick tree;

	public void setUp() {
		this.tree = new AhoCorasick();
	}

	public void testConstruction() {
		tree.add("hello".toCharArray(), "hello".toCharArray());
		tree.add("hi".toCharArray(), "hi".toCharArray());
		tree.prepare();

		State s0 = tree.getRoot();
		State s1 = s0.get('h');
		State s2 = s1.get('e');
		State s3 = s2.get('l');
		State s4 = s3.get('l');
		State s5 = s4.get('o');
		State s6 = s1.get('i');

		assertEquals(s0, s1.getFail());
		assertEquals(s0, s2.getFail());
		assertEquals(s0, s3.getFail());
		assertEquals(s0, s4.getFail());
		assertEquals(s0, s5.getFail());
		assertEquals(s0, s6.getFail());

		assertEquals(0, s0.getOutputs().size());
		assertEquals(0, s1.getOutputs().size());
		assertEquals(0, s2.getOutputs().size());
		assertEquals(0, s3.getOutputs().size());
		assertEquals(0, s4.getOutputs().size());
		assertEquals(1, s5.getOutputs().size());
		assertEquals(1, s6.getOutputs().size());

		assertTrue(s6 != null);
	}

	public void testExample() {
		tree.add("he".toCharArray(), "he".toCharArray());
		tree.add("she".toCharArray(), "she".toCharArray());
		tree.add("his".toCharArray(), "his".toCharArray());
		tree.add("hers".toCharArray(), "hers".toCharArray());
		assertEquals(10, tree.getRoot().size());
		tree.prepare(); // after prepare, we can't call size()
		State s0 = tree.getRoot();
		State s1 = s0.get('h');
		State s2 = s1.get('e');

		State s3 = s0.get('s');
		State s4 = s3.get('h');
		State s5 = s4.get('e');

		State s6 = s1.get('i');
		State s7 = s6.get('s');

		State s8 = s2.get('r');
		State s9 = s8.get('s');

		assertEquals(s0, s1.getFail());
		assertEquals(s0, s2.getFail());
		assertEquals(s0, s3.getFail());
		assertEquals(s0, s6.getFail());
		assertEquals(s0, s8.getFail());

		assertEquals(s1, s4.getFail());
		assertEquals(s2, s5.getFail());
		assertEquals(s3, s7.getFail());
		assertEquals(s3, s9.getFail());

		assertEquals(0, s1.getOutputs().size());
		assertEquals(0, s3.getOutputs().size());
		assertEquals(0, s4.getOutputs().size());
		assertEquals(0, s6.getOutputs().size());
		assertEquals(0, s8.getOutputs().size());
		assertEquals(1, s2.getOutputs().size());
		assertEquals(1, s7.getOutputs().size());
		assertEquals(1, s9.getOutputs().size());
		assertEquals(2, s5.getOutputs().size());
	}

	public void testStartSearchWithSingleResult() {
		tree.add("apple".toCharArray(), "apple".toCharArray());
		tree.prepare();
		SearchResult result = tree.startSearch("washington cut the apple tree"
				.toCharArray());
		assertEquals(1, result.getOutputs().size());
		assertEquals("apple", new String((char[]) result.getOutputs()
				.iterator().next()));
		assertEquals(24, result.lastIndex);
		assertEquals(null, tree.continueSearch(result));
	}
	
	public void testStartSearchWithUnicodeResult() {
		tree.add("españa".toCharArray(), "españa".toCharArray());
		tree.prepare();
		SearchResult result = tree.startSearch("la campeona del mundo de fútbol es españa"
				.toCharArray());
		assertEquals(1, result.getOutputs().size());
		assertEquals("españa", new String((char[]) result.getOutputs()
				.iterator().next()));
		assertEquals(41, result.lastIndex);
		assertEquals(null, tree.continueSearch(result));
	}

	public void testStartSearchWithAdjacentResults() {
		tree.add("john".toCharArray(), "john".toCharArray());
		tree.add("jane".toCharArray(), "jane".toCharArray());
		tree.prepare();
		SearchResult firstResult = tree.startSearch("johnjane".toCharArray());
		SearchResult secondResult = tree.continueSearch(firstResult);
		assertEquals(null, tree.continueSearch(secondResult));
	}

	public void testStartSearchOnEmpty() {
		tree.add("cipher".toCharArray(), new Integer(0));
		tree.add("zip".toCharArray(), new Integer(1));
		tree.add("nought".toCharArray(), new Integer(2));
		tree.prepare();
		SearchResult result = tree.startSearch("".toCharArray());
		assertEquals(null, result);
	}

	public void testMultipleOutputs() {
		tree.add("x".toCharArray(), "x");
		tree.add("xx".toCharArray(), "xx");
		tree.add("xxx".toCharArray(), "xxx");
		tree.prepare();

		SearchResult result = tree.startSearch("xxx".toCharArray());
		assertEquals(1, result.lastIndex);
		assertEquals(new HashSet<Object>(Arrays.asList(new String[] { "x" })),
				result.getOutputs());

		result = tree.continueSearch(result);
		assertEquals(2, result.lastIndex);
		assertEquals(
				new HashSet<Object>(Arrays.asList(new String[] { "xx", "x" })),
				result.getOutputs());

		result = tree.continueSearch(result);
		assertEquals(3, result.lastIndex);
		assertEquals(
				new HashSet<Object>(Arrays.asList(new String[] { "xxx", "xx",
						"x" })), result.getOutputs());

		assertEquals(null, tree.continueSearch(result));
	}

	public void testIteratorInterface() {
		tree.add("moo".toCharArray(), "moo");
		tree.add("one".toCharArray(), "one");
		tree.add("on".toCharArray(), "on");
		tree.add("ne".toCharArray(), "ne");
		tree.prepare();
		Iterator<SearchResult> iter = tree.progressiveSearch("one moon ago".toCharArray());

		assertTrue(iter.hasNext());
		SearchResult r = (SearchResult) iter.next();
		assertEquals(new HashSet<Object>(Arrays.asList(new String[] { "on" })),
				r.getOutputs());
		assertEquals(2, r.lastIndex);

		assertTrue(iter.hasNext());
		r = (SearchResult) iter.next();
		assertEquals(
				new HashSet<Object>(Arrays.asList(new String[] { "one", "ne" })),
				r.getOutputs());
		assertEquals(3, r.lastIndex);

		assertTrue(iter.hasNext());
		r = (SearchResult) iter.next();
		assertEquals(
				new HashSet<Object>(Arrays.asList(new String[] { "moo" })),
				r.getOutputs());
		assertEquals(7, r.lastIndex);

		assertTrue(iter.hasNext());
		r = (SearchResult) iter.next();
		assertEquals(new HashSet<Object>(Arrays.asList(new String[] { "on" })),
				r.getOutputs());
		assertEquals(8, r.lastIndex);

		assertFalse(iter.hasNext());

		try {
			iter.next();
			fail();
		} catch (NoSuchElementException e) {
		}

	}

	public void largerTextExample() {
		String text = "The ga3 mutant of Arabidopsis is a gibberellin-responsive dwarf. We present data showing that the ga3-1 mutant is deficient in ent-kaurene oxidase activity, the first cytochrome P450-mediated step in the gibberellin biosynthetic pathway. By using a combination of conventional map-based cloning and random sequencing we identified a putative cytochrome P450 gene mapping to the same location as GA3. Relative to the progenitor line, two ga3 mutant alleles contained single base changes generating in-frame stop codons in the predicted amino acid sequence of the P450. A genomic clone spanning the P450 locus complemented the ga3-2 mutant. The deduced GA3 protein defines an additional class of cytochrome P450 enzymes. The GA3 gene was expressed in all tissues examined, RNA abundance being highest in inflorescence tissue.";
		String[] terms = { "microsome", "cytochrome",
				"cytochrome P450 activity", "gibberellic acid biosynthesis",
				"GA3", "cytochrome P450", "oxygen binding", "AT5G25900.1",
				"protein", "RNA", "gibberellin", "Arabidopsis",
				"ent-kaurene oxidase activity", "inflorescence", "tissue", };
		for (int i = 0; i < terms.length; i++) {
			tree.add(terms[i].toCharArray(), terms[i]);
		}
		tree.prepare();

		Set<Object> termsThatHit = new HashSet<>();
		for (Iterator<SearchResult> iter = tree.progressiveSearch(text.toCharArray()); iter
				.hasNext();) {
			SearchResult result = (SearchResult) iter.next();
			termsThatHit.addAll(result.getOutputs());
		}
		assertEquals(
				new HashSet<Object>(Arrays.asList(new String[] { "cytochrome",
						"GA3", "cytochrome P450", "protein", "RNA",
						"gibberellin", "Arabidopsis",
						"ent-kaurene oxidase activity", "inflorescence",
						"tissue", })), termsThatHit);

	}
	
	// Without overlapping
	public void testRemoveOverlapping1() {
		List<OutputResult> outputResults = new ArrayList<>();
		outputResults.add(new OutputResult(new Integer(0), 0, 2));
		outputResults.add(new OutputResult(new Integer(1), 2, 4));
		outputResults.add(new OutputResult(new Integer(2), 5, 6));
		
		assertEquals(3, outputResults.size());
		tree.removeOverlapping(outputResults); // No effect
		assertEquals(3, outputResults.size());
	}
	
	// With a clear overlapping
	public void testRemoveOverlapping2() {
		List<OutputResult> outputResults = new ArrayList<>();
		outputResults.add(new OutputResult(new Integer(0), 0, 2));
		outputResults.add(new OutputResult(new Integer(1), 1, 4));
		outputResults.add(new OutputResult(new Integer(2), 5, 6));
		
		assertEquals(3, outputResults.size());
		tree.removeOverlapping(outputResults);
		assertEquals(2, outputResults.size());
		assertEquals(new Integer(0), outputResults.get(0).getOutput());
		assertEquals(new Integer(2), outputResults.get(1).getOutput());
	}
	
	// With two overlapping, one with the same start index
	public void testRemoveOverlapping3() {
		List<OutputResult> outputResults = new ArrayList<>();
		outputResults.add(new OutputResult(new Integer(0), 0, 2));
		outputResults.add(new OutputResult(new Integer(1), 0, 4));
		outputResults.add(new OutputResult(new Integer(2), 3, 6));
		
		assertEquals(3, outputResults.size());
		tree.removeOverlapping(outputResults);
		assertEquals(1, outputResults.size());
		assertEquals(new Integer(1), outputResults.get(0).getOutput());
	}
	
	public void testCompleteSearchNotOverlapping() {
		tree.add("Apple");
		tree.add("App");
		tree.add("Microsoft");
		tree.add("Mic");
		tree.prepare();
		
		String inputText = "Apple is better than Microsoft";
		List<OutputResult> results = tree.completeSearch(inputText, false, false);
		
		assertEquals(2, results.size());
		assertEquals("Apple", results.get(0).getOutput());
		assertEquals("Microsoft", results.get(1).getOutput());
	}
	
	public void testCompleteSearchOverlapping() {
		tree.add("Apple");
		tree.add("App");
		tree.add("Microsoft");
		tree.add("Mic");
		tree.prepare();
		
		String inputText = "Apple is better than Microsoft";
		List<OutputResult> results = tree.completeSearch(inputText, true, false);
		
		assertEquals(4, results.size());
		assertEquals("App", results.get(0).getOutput());
		assertEquals("Apple", results.get(1).getOutput());
		assertEquals("Mic", results.get(2).getOutput());
		assertEquals("Microsoft", results.get(3).getOutput());
	}
	
	public void testCompleteSearchTokenized1() {
		tree.add("Apple");
		tree.add("e i");
		tree.add("than Microsoft");
		tree.add("Microsoft");
		tree.add("er than");
		tree.prepare();
		
		String inputText = "Apple is better than Microsoft";
		List<OutputResult> results = tree.completeSearch(inputText, false, true); // without overlapping
		
		assertEquals(2, results.size());
		assertEquals("Apple", results.get(0).getOutput());
		assertEquals("than Microsoft", results.get(1).getOutput());
		
		results = tree.completeSearch(inputText, true, true); // with overlapping
		
		assertEquals(3, results.size());
		assertEquals("Apple", results.get(0).getOutput());
		assertEquals("than Microsoft", results.get(1).getOutput());
		assertEquals("Microsoft", results.get(2).getOutput());
	}
	
	public void testCompleteSearchTokenized2() {
		tree.add("Real Madrid");
		tree.add("Madrid");
		tree.add("Barcelona");
		tree.add("Messi");
		tree.add("esp");
		tree.add("o p");
		tree.add("Mes");
		tree.add("Rea");
		tree.prepare();
		
		String inputText = "El Real Madrid no puede fichar a Messi porque es del Barcelona";
		List<OutputResult> results = tree.completeSearch(inputText, false, true); // without overlapping

		assertEquals(3, results.size());
		assertEquals("Real Madrid", results.get(0).getOutput());
		assertEquals("Messi", results.get(1).getOutput());
		assertEquals("Barcelona", results.get(2).getOutput());
	}
	
	public void testCompleteSearchTokenized3() {
		tree.add("comp");
		tree.prepare();
		
		String inputText = "A complete sentence";
		List<OutputResult> results = tree.completeSearch(inputText, false, true); // without overlapping

		assertEquals(0, results.size());
	}
	
	public void testCompleteSearchTokenized4() {
		tree.add("Madrid");
		tree.add("Real");
		tree.add("Real Madrid");
		tree.add("El Real de España");
		tree.prepare();
		
		String inputText = "El Real Madrid no puede fichar a Messi porque es del Barcelona";
		List<OutputResult> results = tree.completeSearch(inputText, false, true); // without overlapping
		
		assertEquals(1, results.size());
		assertEquals("Real Madrid", results.get(0).getOutput());
	}
	
	public void testCompleteSearchTokenized5() {
		tree.add("Microsoft");
		tree.add("than Microsoft");
		tree.add("han Microsoft");
		tree.add("n Microsoft");
		tree.add(" Microsoft");
		tree.prepare();
		
		String inputText = "Apple is better than Microsoft";
		List<OutputResult> results = tree.completeSearch(inputText, true, true); // with overlapping
		
		assertEquals(2, results.size());
		assertEquals("than Microsoft", results.get(0).getOutput());
		assertEquals("Microsoft", results.get(1).getOutput());
	}
	
	public void testSerializationToByteArray() throws IOException, ClassNotFoundException {
		tree.add("Apple");
		tree.add("Microsoft");
		tree.prepare();
		
		// Basic functions
		String inputText = "Apple is better than Microsoft";
		List<OutputResult> results = tree.completeSearch(inputText, true, false);
		assertEquals(2, results.size());
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		
		// Serialization
		oos.writeObject(tree);
		
		// Deserialization
		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
		tree = (AhoCorasick) ois.readObject();
		
		// Checking the deserialization
		results = tree.completeSearch(inputText, true, false);
		assertEquals(2, results.size());
	}
	
	public void testSerializationToFile() throws IOException, ClassNotFoundException {
		tree.add("Apple");
		tree.add("Microsoft");
		tree.prepare();
		
		// Basic functions
		String inputText = "Apple is better than Microsoft";
		List<OutputResult> results = tree.completeSearch(inputText, true, false);
		assertEquals(2, results.size());
		
		File file = File.createTempFile("aho", "corasick");
		FileOutputStream fos = new FileOutputStream(file);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		
		// Serialization
		oos.writeObject(tree);
		
		// Deserialization
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
		tree = (AhoCorasick) ois.readObject();
		
		// Checking the deserialization
		results = tree.completeSearch(inputText, true, false);
		assertEquals(2, results.size());
	}
}
