package com.utils

import static com.kms.katalon.core.checkpoint.CheckpointFactory.findCheckpoint
import static com.kms.katalon.core.testcase.TestCaseFactory.findTestCase
import static com.kms.katalon.core.testdata.TestDataFactory.findTestData
import static com.kms.katalon.core.testobject.ObjectRepository.findTestObject
import static com.kms.katalon.core.testobject.ObjectRepository.findWindowsObject

import com.kms.katalon.core.annotation.Keyword
import com.kms.katalon.core.checkpoint.Checkpoint
import com.kms.katalon.core.cucumber.keyword.CucumberBuiltinKeywords as CucumberKW
import com.kms.katalon.core.mobile.keyword.MobileBuiltInKeywords as Mobile
import com.kms.katalon.core.model.FailureHandling
import com.kms.katalon.core.testcase.TestCase
import com.kms.katalon.core.testdata.TestData
import com.kms.katalon.core.testobject.TestObject
import com.kms.katalon.core.webservice.keyword.WSBuiltInKeywords as WS
import com.kms.katalon.core.webui.keyword.WebUiBuiltInKeywords as WebUI
import com.kms.katalon.core.windows.keyword.WindowsBuiltinKeywords as Windows
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Collections
import java.util.Properties
import com.kms.katalon.core.annotation.Keyword

import internal.GlobalVariable

public class KafkaConsumerHelper {

	@Keyword
	def consumeKafkaMessage(String bootstrapServers, String topic, String groupId) {
		// 1. Konfigurasi Properties Kafka Consumer
		Properties props = new Properties()
		props.setProperty("bootstrap.servers", bootstrapServers)
		props.setProperty("group.id", groupId)
		props.setProperty("key.deserializer", StringDeserializer.class.getName())
		props.setProperty("value.deserializer", StringDeserializer.class.getName())
		props.setProperty("auto.offset.reset", "earliest") // Membaca dari awal jika tidak ada offset

		// 2. Inisialisasi Consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)
		
		try {
			// 3. Subscribe ke Topic
			consumer.subscribe(Collections.singletonList(topic))
			println("Subscribed to topic: " + topic)

			// 4. Polling pesan (Mencoba membaca data)
			// Kita set timeout pendek untuk demo test (misal 5 detik)
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000))
			
			if (records.isEmpty()) {
				println("Tidak ada pesan ditemukan pada topic tersebut.")
				return "No Data"
			}

			// 5. Ambil pesan pertama untuk validasi
			for (ConsumerRecord<String, String> record : records) {
				println("Message Received: Key = " + record.key() + ", Value = " + record.value())
				// Return value pesan pertama untuk di-verify di Test Case
				return record.value()
			}
		} catch (Exception e) {
			e.printStackTrace()
		} finally {
			consumer.close()
		}
		return null
	}
}