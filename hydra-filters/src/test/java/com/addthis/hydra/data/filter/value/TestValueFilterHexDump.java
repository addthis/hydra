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

import java.io.IOException;

import java.util.Map;

import com.addthis.codec.config.Configs;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestValueFilterHexDump {

    /**
     * Pangrams from http://clagnut.com/blog/2380.
     * Hexadecimal representation from hexdump -e '16/1 "%02x " " "'
     */
    private static ImmutableMap<String, String> PANGRAMS = ImmutableMap
            .<String, String>builder()
            .put("Ах чудна българска земьо, полюшвай цъфтящи жита", "d0 90 d1 85 20 d1 87 d1 83 d0 b4 d0 bd d0 b0 20 d0 b1 d1 8a d0 bb d0 b3 d0 b0 d1 80 d1 81 d0 ba d0 b0 20 d0 b7 d0 b5 d0 bc d1 8c d0 be 2c 20 d0 bf d0 be d0 bb d1 8e d1 88 d0 b2 d0 b0 d0 b9 20 d1 86 d1 8a d1 84 d1 82 d1 8f d1 89 d0 b8 20 d0 b6 d0 b8 d1 82 d0 b0")
            .put("Victor jagt zwölf Boxkämpfer quer über den großen Sylter Deich", "56 69 63 74 6f 72 20 6a 61 67 74 20 7a 77 c3 b6 6c 66 20 42 6f 78 6b c3 a4 6d 70 66 65 72 20 71 75 65 72 20 c3 bc 62 65 72 20 64 65 6e 20 67 72 6f c3 9f 65 6e 20 53 79 6c 74 65 72 20 44 65 69 63 68")
            .put("Ταχίστη αλώπηξ βαφής ψημένη γη, δρασκελίζει υπέρ νωθρού κυνός","ce a4 ce b1 cf 87 ce af cf 83 cf 84 ce b7 20 ce b1 ce bb cf 8e cf 80 ce b7 ce be 20 ce b2 ce b1 cf 86 ce ae cf 82 20 cf 88 ce b7 ce bc ce ad ce bd ce b7 20 ce b3 ce b7 2c 20 ce b4 cf 81 ce b1 cf 83 ce ba ce b5 ce bb ce af ce b6 ce b5 ce b9 20 cf 85 cf 80 ce ad cf 81 20 ce bd cf 89 ce b8 cf 81 ce bf cf 8d 20 ce ba cf 85 ce bd cf 8c cf 82")
            .put("Jeżu klątw, spłódź Finom część gry hańb!", "4a 65 c5 bc 75 20 6b 6c c4 85 74 77 2c 20 73 70 c5 82 c3 b3 64 c5 ba 20 46 69 6e 6f 6d 20 63 7a c4 99 c5 9b c4 87 20 67 72 79 20 68 61 c5 84 62 21")
            .put("Эх, чужак, общий съём цен шляп (юфть) – вдрызг!", "d0 ad d1 85 2c 20 d1 87 d1 83 d0 b6 d0 b0 d0 ba 2c 20 d0 be d0 b1 d1 89 d0 b8 d0 b9 20 d1 81 d1 8a d1 91 d0 bc 20 d1 86 d0 b5 d0 bd 20 d1 88 d0 bb d1 8f d0 bf 20 28 d1 8e d1 84 d1 82 d1 8c 29 20 e2 80 93 20 d0 b2 d0 b4 d1 80 d1 8b d0 b7 d0 b3 21")
            .put("視野無限廣，窗外有藍天", "e8 a6 96 e9 87 8e e7 84 a1 e9 99 90 e5 bb a3 ef bc 8c e7 aa 97 e5 a4 96 e6 9c 89 e8 97 8d e5 a4 a9")
            .build();

    @Test
    public void hexDump() throws IOException {
        ValueFilterHexDump filter = Configs.decodeObject(ValueFilterHexDump.class, "");
        for (Map.Entry<String, String> entry : PANGRAMS.entrySet()) {
            assertEquals(entry.getValue(), filter.filter(entry.getKey()));
        }
    }

    @Test
    public void reverse() throws IOException {
        ValueFilterHexDump filter = Configs.decodeObject(ValueFilterHexDump.class, "reverse: true");
        for (Map.Entry<String, String> entry : PANGRAMS.entrySet()) {
            assertEquals(entry.getKey(), filter.filter(entry.getValue()));
        }
    }


}
