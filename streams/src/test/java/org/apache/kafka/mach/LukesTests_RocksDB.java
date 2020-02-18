/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.mach;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.RocksDBStoreTest;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecordingTrigger;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import static org.junit.Assert.*;

@SuppressWarnings("unchecked")
public class LukesTests_RocksDB
{

    /**
     * The scope for metrics for the rocks DB.
     */
    private final static String METRICS_SCOPE = "metrics-scope";


    /**
     * This gets the name of the running test.
     */
    @Rule
    public TestName testName = new TestName();

    /**
     * The folder where our test output can go.
     */
    @Rule
    public TestFolder testFolder = new TestFolder();

    /**
     * This is a path for the specific test.
     * You can use this to write output for the test.
     */
    Path testPath;

    /**
     * This is the rocks DB store that we are testing.
     */
    KeyValueBytesStoreSupplier rocksDBStoreSupplier;

    /**
     * The rocksDB store that is being tested.
     */
    KeyValueStore<Bytes, byte[]> store;

    /**
     * The context for the database.
     */
    private InternalProcessorContext context;

    @Before
    public void setUp()
    {
        // Capture the current timestamp:
        LocalDateTime now = LocalDateTime.now();

        // Get the time stamp:
        String timestamp = now.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

        // Create a temporary folder for this run:
        testPath = testFolder.getTestFolder();

        // Create the
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, RocksDBStoreTest.MockRocksDbConfigSetter.class);
        context = new InternalMockProcessorContext(testPath.toFile(),
                Serdes.String(),
                Serdes.String(),
                new StreamsConfig(props));
        rocksDBStoreSupplier = Stores.persistentKeyValueStore(testName.getMethodName());
        store = rocksDBStoreSupplier.get();
        context.metrics().setRocksDBMetricsRecordingTrigger(new RocksDBMetricsRecordingTrigger());
        store.init(context, store);
    }

    @After
    public void tearDown()
    {
        store.flush();
        store.close();
    }

    @Test
    public void createRocksDB()
    {
        assertNotNull(store);
        assertTrue(store.isOpen());
    }

    @Test
    public void writeToRocksDB()
    {
        store.put(Bytes.wrap(new byte[5]), "Hello World!".getBytes(StandardCharsets.UTF_8));
    }
}
