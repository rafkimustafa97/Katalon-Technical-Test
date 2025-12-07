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
import static com.kms.katalon.core.checkpoint.CheckpointFactory.findCheckpoint
import static com.kms.katalon.core.testcase.TestCaseFactory.findTestCase
import static com.kms.katalon.core.testdata.TestDataFactory.findTestData
import static com.kms.katalon.core.testobject.ObjectRepository.findTestObject
import com.kms.katalon.core.webservice.keyword.WSBuiltInKeywords as WS
import internal.GlobalVariable as GlobalVariable

import static com.kms.katalon.core.checkpoint.CheckpointFactory.findCheckpoint
import static com.kms.katalon.core.testcase.TestCaseFactory.findTestCase
import static com.kms.katalon.core.testdata.TestDataFactory.findTestData
import static com.kms.katalon.core.testobject.ObjectRepository.findTestObject
import com.kms.katalon.core.webservice.keyword.WSBuiltInKeywords as WS
import internal.GlobalVariable as GlobalVariable

// --- BAGIAN 1: KATALON SEBAGAI PRODUCER (POST) ---
println "Starting Producer Action..."
def responsePost = WS.sendRequest(findTestObject('POST_CreateUser'))

// Verifikasi server menerima data (Status 201 Created)
WS.verifyResponseStatusCode(responsePost, 201)

// Verifikasi response body mengandung data title 'foo' yang kita kirim
WS.verifyElementPropertyValue(responsePost, 'title', 'foo')
println "Producer Action Success. Resource created."

// --- BAGIAN 2: KATALON SEBAGAI CONSUMER (GET) ---
println "Starting Consumer Action..."
def responseGet = WS.sendRequest(findTestObject('GET_SingleUser'))

// Verifikasi data berhasil diambil (Status 200 OK)
WS.verifyResponseStatusCode(responseGet, 200)

// Validasi bahwa data yang diambil memiliki ID 1 (sesuai URL posts/1)
WS.verifyElementPropertyValue(responseGet, 'id', 1)
println "Consumer Action Success. Data validated."