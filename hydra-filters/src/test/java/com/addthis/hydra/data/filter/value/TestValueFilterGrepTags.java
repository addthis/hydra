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
package com.addthis.hydra.data.filter.value;

import javax.annotation.Syntax;

import java.io.IOException;

import com.addthis.bundle.value.ValueFactory;
import com.addthis.codec.config.Configs;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestValueFilterGrepTags {

    @Test
    public void basic() throws IOException {
        @Syntax("HTML") String html =
                "<html> <head> <meta charset=\"UTF-8\" /> <title>Peacock</title> " +
                "<LINK href=\"lib/bootstrap/bootstrap.css\" rel=\"stylesheet\" type=\"text/css\"> " +
                "<script type='text/javascript' src='lib/jquery.js'></script> " +
                "<script type='text/javascript' src='lib/underscore.js'></script> " +
                "<script type='text/javascript' src='lib/backbone.js'></script> " +
                "<script type='text/javascript' src='lib/moment.js'></script> " +
                "<script type='text/javascript' src='lib/bootstrap/bootstrap-dropdown.js'></script> " +
                "<script src='peacock.js'></script> </head> <body> <div id='peacock'></div> " +
                "<!--script src='demo.js'></script--> </body> </html>";

        ValueFilterGrepTags grepTags = Configs.decodeObject(
                ValueFilterGrepTags.class, "values = [bootstrap, jquery], tagAttrs = src, tagName = script");
        assertNotNull(grepTags.filterValue(ValueFactory.create(html)));

        ValueFilterGrepTags failGrepTags = Configs.decodeObject(
                ValueFilterGrepTags.class, "values = [foo, bar], tagAttrs = src, tagName = script");
        assertNull(failGrepTags.filterValue(ValueFactory.create(html)));
    }
}

