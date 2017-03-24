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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Vinicius Carvalho
 */
public class CSV2JsonProcessorIntegrationTests {
	/**
	 * Integration test to verify that the processor can actually download remote URL such as http:// endpoints that return CSV files
	 * @throws Exception
	 */
	@Test
	public void integrationTests() throws Exception {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(ProcessorApplication.class,
				"--server.port=0", "--spring.jmx.enabled=false", "--csv2json.includes=first_name,last_name","--security.basic.enabled=false");
		Csv2JsonProcessorProperties processorProperties = applicationContext.getBean(Csv2JsonProcessorProperties.class);
		Environment env = applicationContext.getEnvironment();
		Processor processor = applicationContext.getBean(Processor.class);
		MessageCollector collector = applicationContext.getBean(MessageCollector.class);
		processor.input().send(MessageBuilder.withPayload("http://localhost:"+env.getProperty("local.server.port")+"/csv").build());
		Message output = collector.forChannel(processor.output()).poll(2000, TimeUnit.MILLISECONDS);
		Map<String,String> payload = (Map<String, String>) output.getPayload();
		Assert.assertNull(payload.get("identity"));
		Assert.assertNotNull(payload.get("first_name"));
		Assert.assertNotNull(payload.get("last_name"));
		applicationContext.close();
	}

	@EnableAutoConfiguration
	@Import(Csv2jsonProcessorConfiguration.class)
	@RestController
	public static class ProcessorApplication{

		@RequestMapping("/csv")
		public String getCSV(){
			return "first_name,last_name,identity\nPeter,Parker,Spiderman";
		}

	}
}
