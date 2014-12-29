package com.addthis.hydra.task.map;

import java.io.IOException;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueTranslationException;
import com.addthis.codec.config.Configs;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RepeatBuilderTest {

    StreamEmitter emitter;
    Bundle bundle;
    RepeatBuilder builder;

    private static final String repeatField = "reps";

    @Before
    public void setup() {
        emitter = mock(StreamEmitter.class);
        bundle = new ListBundle();
    }

    @Test
    public void basicFunctionality() throws IOException {
        builder = Configs.decodeObject(RepeatBuilder.class, String.format("{repeatField:\"%s\"}", repeatField));
        int reps = 5;
        Bundle bundle = makeBundle(reps);
        builder.process(bundle, emitter);
        ArgumentCaptor<Bundle> bundleCapture = ArgumentCaptor.forClass(Bundle.class);
        verify(emitter, times(reps)).emit(bundleCapture.capture());
    }

    @Test
    public void defaultRepeatCount() throws IOException {
        int defaultRepeatCount = 2;
        builder = Configs.decodeObject(RepeatBuilder.class, String.format("{repeatField:\"%s\", defaultRepeats:%d}", repeatField, defaultRepeatCount));
        Bundle bundle = new ListBundle();
        builder.process(bundle, emitter);
        ArgumentCaptor<Bundle> bundleCapture = ArgumentCaptor.forClass(Bundle.class);
        verify(emitter, times(defaultRepeatCount)).emit(bundleCapture.capture());
    }

    @Test(expected = ValueTranslationException.class)
    public void escalateOnParseFailure() throws IOException {
        builder = Configs.decodeObject(RepeatBuilder.class, String.format("{repeatField:\"%s\", failOnParseException: true}", repeatField));
        Bundle bundle = new ListBundle(ImmutableMap.of(repeatField, ValueFactory.create("zzzzz")));
        builder.process(bundle, emitter);
    }

    private static Bundle makeBundle(int reps) {
        return new ListBundle(ImmutableMap.of(repeatField, ValueFactory.create(reps)));
    }
}
