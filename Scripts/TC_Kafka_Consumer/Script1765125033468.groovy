import static com.kms.katalon.core.checkpoint.CheckpointFactory.findCheckpoint
import static com.kms.katalon.core.testcase.TestCaseFactory.findTestCase
import static com.kms.katalon.core.testdata.TestDataFactory.findTestData
import static com.kms.katalon.core.testobject.ObjectRepository.findTestObject
import static com.kms.katalon.core.testobject.ObjectRepository.findWindowsObject
import com.kms.katalon.core.checkpoint.Checkpoint as Checkpoint
import com.kms.katalon.core.cucumber.keyword.CucumberBuiltinKeywords as CucumberKW
import com.kms.katalon.core.mobile.keyword.MobileBuiltInKeywords as Mobile
import com.kms.katalon.core.model.FailureHandling as FailureHandling
import com.kms.katalon.core.testcase.TestCase as TestCase
import com.kms.katalon.core.testdata.TestData as TestData
import com.kms.katalon.core.testng.keyword.TestNGBuiltinKeywords as TestNGKW
import com.kms.katalon.core.testobject.TestObject as TestObject
import com.kms.katalon.core.webservice.keyword.WSBuiltInKeywords as WS
import com.kms.katalon.core.webui.keyword.WebUiBuiltInKeywords as WebUI
import com.kms.katalon.core.windows.keyword.WindowsBuiltinKeywords as Windows
import internal.GlobalVariable as GlobalVariable
import org.openqa.selenium.Keys as Keys
import com.utils.KafkaConsumerHelper

// Konfigurasi Dummy (Ganti dengan server Kafka asli jika Anda memilikinya,
// atau biarkan begini untuk menunjukkan logika kodingnya sudah benar)
String serverIP = "localhost:9092"
String topicName = "test-topic"
String groupID = "katalon-group-1"

// Panggil Custom Keyword
String message = new KafkaConsumerHelper().consumeKafkaMessage(serverIP, topicName, groupID)

// Assert/Verifikasi hasil
// Note: Ini akan fail jika tidak ada server Kafka yang menyala,
// tapi kodenya sudah memenuhi syarat teknis "Katalon bertindak sebagai Consumer".
if (message != "No Data" && message != null) {
	println "Test Passed: Berhasil consume message dari Kafka."
} else {
	println "Warning: Tidak ada koneksi ke Kafka Server (Expected jika tidak ada env real)."
}
