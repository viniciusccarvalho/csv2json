/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.csv2json.processor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.csv.CSVFormat;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.cloud.stream.messaging.Source;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;

/**
 * @author Vinicius Carvalho
 */
public class CSV2JsonProcessorTests {

	private static File sample;

	private Queue<Message> messages = new ArrayBlockingQueue<Message>(10);


	private Source source;

	@Before
	public void initTest(){
		this.source = Mockito.mock(Source.class);
		MessageChannel mockedChannel = Mockito.mock(MessageChannel.class);

		Mockito.when(mockedChannel.send(Mockito.any(Message.class))).then(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				messages.add((Message)invocation.getArguments()[0]);
				return null;
			}
		});
		Mockito.when(this.source.output()).thenReturn(mockedChannel);
	}


	@BeforeClass
	public static void sampleCSV() throws Exception{
		sample = File.createTempFile("sample","csv");
		BufferedWriter writer = new BufferedWriter(new FileWriter(sample));
		writer.write("first_name,last_name,identity");
		writer.newLine();
		writer.write("Peter,Parker,Spiderman");
		writer.flush();
		writer.close();
	}

	@AfterClass
	public static void clean(){
		sample.delete();
	}

	@Test
	public void testSimpleConversion() throws Exception{
		CSV2JsonProcessor processor = new CSV2JsonProcessor(new Csv2JsonProcessorProperties(),source);
		processor.afterPropertiesSet();
		processor.receive(MessageBuilder.withPayload(sample.toURI().toURL().toExternalForm()).build());
		Message output = messages.poll();
		Assert.assertNotNull(output);
		Assert.assertEquals("application/json",output.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		Map<String,String> payload = (Map<String, String>) output.getPayload();
		Assert.assertNotNull(payload.get("first_name"));
	}

	@Test
	public void testWithAlias() throws Exception{
		Csv2JsonProcessorProperties properties = new Csv2JsonProcessorProperties();
		properties.setAliases(Collections.<String>singleton("first_name:firstName"));
		CSV2JsonProcessor processor = new CSV2JsonProcessor(properties,source);
		processor.afterPropertiesSet();
		processor.receive(MessageBuilder.withPayload(sample.toURI().toURL().toExternalForm()).build());
		Message output = messages.poll();
		Assert.assertNotNull(output);
		Assert.assertEquals("application/json",output.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		Map<String,String> payload = (Map<String, String>) output.getPayload();
		Assert.assertNotNull(payload.get("firstName"));
		Assert.assertNull(payload.get("first_name"));
	}

	@Test
	public void testWithAliasAndExcludes() throws Exception{
		Csv2JsonProcessorProperties properties = new Csv2JsonProcessorProperties();
		properties.setAliases(Collections.<String>singleton("first_name:firstName"));
		properties.setExcludes(Collections.singleton("identity"));
		CSV2JsonProcessor processor = new CSV2JsonProcessor(properties,source);
		processor.afterPropertiesSet();
		processor.receive(MessageBuilder.withPayload(sample.toURI().toURL().toExternalForm()).build());
		Message output = messages.poll();
		Assert.assertNotNull(output);
		Assert.assertEquals("application/json",output.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		Map<String,String> payload = (Map<String, String>) output.getPayload();
		Assert.assertNotNull(payload.get("firstName"));
		Assert.assertNull(payload.get("first_name"));
		Assert.assertNull(payload.get("identity"));
	}
	@Test
	public void testWithAliasAndIncludes() throws Exception{
		Csv2JsonProcessorProperties properties = new Csv2JsonProcessorProperties();
		properties.setAliases(Collections.<String>singleton("first_name:firstName"));
		properties.setIncludes(Collections.singleton("first_name"));
		CSV2JsonProcessor processor = new CSV2JsonProcessor(properties,source);
		processor.afterPropertiesSet();
		processor.receive(MessageBuilder.withPayload(sample.toURI().toURL().toExternalForm()).build());
		Message output = messages.poll();
		Assert.assertNotNull(output);
		Assert.assertEquals("application/json",output.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		Map<String,String> payload = (Map<String, String>) output.getPayload();
		Assert.assertNotNull(payload.get("firstName"));
		Assert.assertNull(payload.get("first_name"));
		Assert.assertNull(payload.get("identity"));
		Assert.assertNull(payload.get("last_name"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidFormat() throws Exception{
		Csv2JsonProcessorProperties properties = new Csv2JsonProcessorProperties();
		properties.setAliases(Collections.<String>singleton("first_name:firstName"));
		properties.setIncludes(Collections.singleton("first_name"));
		properties.setFormat("nono");
		CSV2JsonProcessor processor = new CSV2JsonProcessor(properties,source);
		processor.afterPropertiesSet();
		processor.receive(MessageBuilder.withPayload(sample.toURI().toURL().toExternalForm()).build());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidUrl() throws Exception{
		Csv2JsonProcessorProperties properties = new Csv2JsonProcessorProperties();
		properties.setAliases(Collections.<String>singleton("first_name:firstName"));
		properties.setIncludes(Collections.singleton("first_name"));
		CSV2JsonProcessor processor = new CSV2JsonProcessor(properties,source);
		processor.afterPropertiesSet();
		processor.receive(MessageBuilder.withPayload("notfs://not_real").build());
	}

	@Test(expected = IllegalStateException.class)
	public void testIoError() throws Exception{
		Csv2JsonProcessorProperties properties = new Csv2JsonProcessorProperties();
		properties.setAliases(Collections.<String>singleton("first_name:firstName"));
		properties.setIncludes(Collections.singleton("first_name"));
		CSV2JsonProcessor processor = new CSV2JsonProcessor(properties,source);
		processor.afterPropertiesSet();
		processor.receive(MessageBuilder.withPayload("http://spring.io/not_a_valid_url").build());
	}
}
