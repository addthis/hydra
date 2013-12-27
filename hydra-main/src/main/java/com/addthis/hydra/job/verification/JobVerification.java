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
package com.addthis.hydra.job.verification;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import com.addthis.codec.CodecExceptionLineNumber;
import com.addthis.codec.CodecJSON;
import com.addthis.hydra.task.run.TaskRunnable;
import com.addthis.hydra.task.run.TaskRunner;
import com.addthis.maljson.JSONObject;

public class JobVerification {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        StringBuilder builder = new StringBuilder();
        while (scanner.hasNext()) {
            builder.append(scanner.nextLine());
            builder.append("\n");
        }
        String input = builder.toString();
        input = input.trim();
        if (input.length() == 0) {
            return;
        }
        List<CodecExceptionLineNumber> exceptions = new ArrayList<>();
        try {
            JSONObject jo = new JSONObject(input);
            TaskRunner.initClasses(jo);
            jo.remove("classes");
            CodecJSON.decodeObject(TaskRunnable.class, jo, exceptions);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        for (CodecExceptionLineNumber ex : exceptions) {
            System.out.println(ex.getMessage());
        }
    }


}
