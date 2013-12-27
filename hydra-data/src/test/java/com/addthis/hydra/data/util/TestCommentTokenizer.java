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
package com.addthis.hydra.data.util;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestCommentTokenizer {

    @Test
    public void testNoTokens() {
        List<String> content = new ArrayList<>();
        List<String> delimiters = new ArrayList<>();
        String input = "";
        CommentTokenizer tokenizer = new CommentTokenizer(input);
        tokenizer.tokenize(content, delimiters);

        assertEquals(1, content.size());
        assertEquals(1, delimiters.size());
        assertEquals("", delimiters.get(0));
        assertEquals("", content.get(0));

        content = new ArrayList<>();
        delimiters = new ArrayList<>();
        input = "hello world";
        tokenizer = new CommentTokenizer(input);
        tokenizer.tokenize(content, delimiters);

        assertEquals(1, content.size());
        assertEquals(1, delimiters.size());
        assertEquals("", delimiters.get(0));
        assertEquals("hello world", content.get(0));
    }

    @Test
    public void testSingleLineComment() {
        List<String> content = new ArrayList<>();
        List<String> delimiters = new ArrayList<>();
        String input = "hello world // foo bar \nbaz";
        CommentTokenizer tokenizer = new CommentTokenizer(input);
        tokenizer.tokenize(content, delimiters);
        assertEquals(3, content.size());
        assertEquals(3, delimiters.size());
        assertEquals("//", delimiters.get(0));
        assertEquals("\n", delimiters.get(1));
        assertEquals("", delimiters.get(2));
        assertEquals("hello world ", content.get(0));
        assertEquals(" foo bar ", content.get((1)));
        assertEquals("baz", content.get(2));

        content = new ArrayList<>();
        delimiters = new ArrayList<>();
        input = "// foo bar\n";
        tokenizer = new CommentTokenizer(input);
        tokenizer.tokenize(content, delimiters);
        assertEquals(2, content.size());
        assertEquals(2, delimiters.size());
        assertEquals("//", delimiters.get(0));
        assertEquals("\n", delimiters.get(1));
        assertEquals("", content.get(0));
        assertEquals(" foo bar", content.get((1)));

        content = new ArrayList<>();
        delimiters = new ArrayList<>();
        input = "// foo bar";
        tokenizer = new CommentTokenizer(input);
        tokenizer.tokenize(content, delimiters);
        assertEquals(2, content.size());
        assertEquals(2, delimiters.size());
        assertEquals("//", delimiters.get(0));
        assertEquals("", delimiters.get(1));

        assertEquals("", content.get(0));
        assertEquals(" foo bar", content.get((1)));
    }

    @Test
    public void testMultiLineComment() {
        List<String> content = new ArrayList<>();
        List<String> delimiters = new ArrayList<>();
        String input = "hello world /* foo bar */baz";
        CommentTokenizer tokenizer = new CommentTokenizer(input);
        tokenizer.tokenize(content, delimiters);
        assertEquals(3, content.size());
        assertEquals(3, delimiters.size());
        assertEquals("/*", delimiters.get(0));
        assertEquals("*/", delimiters.get(1));
        assertEquals("", delimiters.get(2));
        assertEquals("hello world ", content.get(0));
        assertEquals(" foo bar ", content.get((1)));
        assertEquals("baz", content.get(2));

        content = new ArrayList<>();
        delimiters = new ArrayList<>();
        input = "hello world /* foo bar baz";
        tokenizer = new CommentTokenizer(input);
        tokenizer.tokenize(content, delimiters);
        assertEquals(2, content.size());
        assertEquals(2, delimiters.size());
        assertEquals("/*", delimiters.get(0));
        assertEquals("", delimiters.get(1));
        assertEquals("hello world ", content.get(0));
        assertEquals(" foo bar baz", content.get((1)));

        content = new ArrayList<>();
        delimiters = new ArrayList<>();
        input = "hello world /* foo bar */ baz /* a b \nc d */ e";
        tokenizer = new CommentTokenizer(input);
        tokenizer.tokenize(content, delimiters);
        assertEquals(5, content.size());
        assertEquals(5, delimiters.size());
        assertEquals("/*", delimiters.get(0));
        assertEquals("*/", delimiters.get(1));
        assertEquals("/*", delimiters.get(2));
        assertEquals("*/", delimiters.get(3));
        assertEquals("", delimiters.get(4));
        assertEquals("hello world ", content.get(0));
        assertEquals(" foo bar ", content.get((1)));
        assertEquals(" baz ", content.get(2));
        assertEquals(" a b \nc d ", content.get(3));
        assertEquals(" e", content.get(4));
    }

    @Test
    public void testParameter() {
        List<String> content = new ArrayList<>();
        List<String> delimiters = new ArrayList<>();
        String input = "hello world %[foo]% bar baz";
        CommentTokenizer tokenizer = new CommentTokenizer(input);
        tokenizer.tokenize(content, delimiters);
        assertEquals(3, content.size());
        assertEquals(3, delimiters.size());
        assertEquals("%[", delimiters.get(0));
        assertEquals("]%", delimiters.get(1));
        assertEquals("", delimiters.get(2));
        assertEquals("hello world ", content.get(0));
        assertEquals("foo", content.get((1)));
        assertEquals(" bar baz", content.get(2));

        content = new ArrayList<>();
        delimiters = new ArrayList<>();
        input = "hello world %[foo:// a b]% bar baz";
        tokenizer = new CommentTokenizer(input);
        tokenizer.tokenize(content, delimiters);
        assertEquals(3, content.size());
        assertEquals(3, delimiters.size());
        assertEquals("%[", delimiters.get(0));
        assertEquals("]%", delimiters.get(1));
        assertEquals("", delimiters.get(2));
        assertEquals("hello world ", content.get(0));
        assertEquals("foo:// a b", content.get((1)));
        assertEquals(" bar baz", content.get(2));
    }

    @Test
    public void testDoubleQuotes() {
        List<String> content = new ArrayList<>();
        List<String> delimiters = new ArrayList<>();
        String input = "hello world \" foo bar \"baz";
        CommentTokenizer tokenizer = new CommentTokenizer(input);
        tokenizer.tokenize(content, delimiters);
        assertEquals(3, content.size());
        assertEquals(3, delimiters.size());
        assertEquals("\"", delimiters.get(0));
        assertEquals("\"", delimiters.get(1));
        assertEquals("", delimiters.get(2));
        assertEquals("hello world ", content.get(0));
        assertEquals(" foo bar ", content.get((1)));
        assertEquals("baz", content.get(2));

        content = new ArrayList<>();
        delimiters = new ArrayList<>();
        input = "hello world \" foo bar baz";
        tokenizer = new CommentTokenizer(input);
        tokenizer.tokenize(content, delimiters);
        assertEquals(2, content.size());
        assertEquals(2, delimiters.size());
        assertEquals("\"", delimiters.get(0));
        assertEquals("", delimiters.get(1));
        assertEquals("hello world ", content.get(0));
        assertEquals(" foo bar baz", content.get((1)));

        content = new ArrayList<>();
        delimiters = new ArrayList<>();
        input = "\\\"hello world\\\"";
        tokenizer = new CommentTokenizer(input);
        tokenizer.tokenize(content, delimiters);
        assertEquals(1, content.size());
        assertEquals(1, delimiters.size());
        assertEquals("", delimiters.get(0));
        assertEquals("\\\"hello world\\\"", content.get(0));

        content = new ArrayList<>();
        delimiters = new ArrayList<>();
        input = "hello world \" foo \\\"abc bar \"baz";
        tokenizer = new CommentTokenizer(input);
        tokenizer.tokenize(content, delimiters);
        assertEquals(3, content.size());
        assertEquals(3, delimiters.size());
        assertEquals("\"", delimiters.get(0));
        assertEquals("\"", delimiters.get(1));
        assertEquals("", delimiters.get(2));
        assertEquals("hello world ", content.get(0));
        assertEquals(" foo \\\"abc bar ", content.get((1)));
        assertEquals("baz", content.get(2));
    }

    @Test
    public void testSingleQuotes() {
        List<String> content = new ArrayList<>();
        List<String> delimiters = new ArrayList<>();
        String input = "hello world \' foo bar \'baz";
        CommentTokenizer tokenizer = new CommentTokenizer(input);
        tokenizer.tokenize(content, delimiters);
        assertEquals(3, content.size());
        assertEquals(3, delimiters.size());
        assertEquals("\'", delimiters.get(0));
        assertEquals("\'", delimiters.get(1));
        assertEquals("", delimiters.get(2));
        assertEquals("hello world ", content.get(0));
        assertEquals(" foo bar ", content.get((1)));
        assertEquals("baz", content.get(2));

        content = new ArrayList<>();
        delimiters = new ArrayList<>();
        input = "hello world \' foo bar baz";
        tokenizer = new CommentTokenizer(input);
        tokenizer.tokenize(content, delimiters);
        assertEquals(2, content.size());
        assertEquals(2, delimiters.size());
        assertEquals("\'", delimiters.get(0));
        assertEquals("", delimiters.get(1));
        assertEquals("hello world ", content.get(0));
        assertEquals(" foo bar baz", content.get((1)));

        content = new ArrayList<>();
        delimiters = new ArrayList<>();
        input = "\\\'hello world\\\'";
        tokenizer = new CommentTokenizer(input);
        tokenizer.tokenize(content, delimiters);
        assertEquals(1, content.size());
        assertEquals(1, delimiters.size());
        assertEquals("", delimiters.get(0));
        assertEquals("\\\'hello world\\\'", content.get(0));

        content = new ArrayList<>();
        delimiters = new ArrayList<>();
        input = "hello world \' foo \\\'abc bar \'baz";
        tokenizer = new CommentTokenizer(input);
        tokenizer.tokenize(content, delimiters);
        assertEquals(3, content.size());
        assertEquals(3, delimiters.size());
        assertEquals("\'", delimiters.get(0));
        assertEquals("\'", delimiters.get(1));
        assertEquals("", delimiters.get(2));
        assertEquals("hello world ", content.get(0));
        assertEquals(" foo \\\'abc bar ", content.get((1)));
        assertEquals("baz", content.get(2));
    }


    @Test
    public void testQuotesWithSingleLineComment() {
        List<String> content = new ArrayList<>();
        List<String> delimiters = new ArrayList<>();
        String input = "hello world // \" foo bar \"baz";
        CommentTokenizer tokenizer = new CommentTokenizer(input);
        tokenizer.tokenize(content, delimiters);
        assertEquals(2, content.size());
        assertEquals(2, delimiters.size());
        assertEquals("//", delimiters.get(0));
        assertEquals("", delimiters.get(1));
        assertEquals("hello world ", content.get(0));
        assertEquals(" \" foo bar \"baz", content.get((1)));

        content = new ArrayList<>();
        delimiters = new ArrayList<>();
        input = "hello world \"// foo bar\" //baz";
        tokenizer = new CommentTokenizer(input);
        tokenizer.tokenize(content, delimiters);
        assertEquals(4, content.size());
        assertEquals(4, delimiters.size());
        assertEquals("\"", delimiters.get(0));
        assertEquals("\"", delimiters.get(1));
        assertEquals("//", delimiters.get(2));
        assertEquals("", delimiters.get(3));
        assertEquals("hello world ", content.get(0));
        assertEquals("// foo bar", content.get((1)));
        assertEquals(" ", content.get(2));
        assertEquals("baz", content.get(3));
    }

    @Test
    public void testQuotesWithMultiLineComment() {
        List<String> content = new ArrayList<>();
        List<String> delimiters = new ArrayList<>();
        String input = "hello world /* \" foo */ bar baz";
        CommentTokenizer tokenizer = new CommentTokenizer(input);
        tokenizer.tokenize(content, delimiters);
        assertEquals(3, content.size());
        assertEquals(3, delimiters.size());
        assertEquals("/*", delimiters.get(0));
        assertEquals("*/", delimiters.get(1));
        assertEquals("", delimiters.get(2));
        assertEquals("hello world ", content.get(0));
        assertEquals(" \" foo ", content.get((1)));
        assertEquals(" bar baz", content.get((2)));
    }

    @Test
    public void testCommentCombinations() {
        List<String> content = new ArrayList<>();
        List<String> delimiters = new ArrayList<>();
        String input = "hello world /* // \n *//* bar baz */";
        CommentTokenizer tokenizer = new CommentTokenizer(input);

        tokenizer.tokenize(content, delimiters);
        assertEquals(4, content.size());
        assertEquals(4, delimiters.size());
        assertEquals("/*", delimiters.get(0));
        assertEquals("*/", delimiters.get(1));
        assertEquals("/*", delimiters.get(2));
        assertEquals("*/", delimiters.get(3));
        assertEquals("hello world ", content.get(0));
        assertEquals(" // \n ", content.get(1));
        assertEquals("", content.get(2));
        assertEquals(" bar baz ", content.get(3));
    }


}