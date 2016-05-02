// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#include <cstdlib>
#include <climits>
#ifdef _CRTDBG_MAP_ALLOC
#include <crtdbg.h>
#endif

#include "testrunnerswitcher.h"
#include "micromock.h"
#include "micromockcharstararenullterminatedstrings.h"

#include "iothub_client.h"
#include "iothub_message.h"
#include "iothubtransportamqp.h"

#include "iothub_account.h"
#include "iothubtest.h"

#include "azure_c_shared_utility/buffer_.h"
#include "azure_c_shared_utility/threadapi.h"
#include "azure_c_shared_utility/platform.h"
#include "azure_c_shared_utility/iot_logging.h"

#ifdef MBED_BUILD_TIMESTAMP
#include "certs.h"
#endif

static MICROMOCK_GLOBAL_SEMAPHORE_HANDLE g_dllByDll;
static bool g_callbackRecv = false;

const char* TEST_EVENT_DATA_FMT = "{\"data\":\"%.24s\",\"id\":\"%d\"}";
const char* TEST_MESSAGE_DATA_FMT = "{\"notifyData\":\"%.24s\",\"id\":\"%d\"}";

static size_t g_iotHubTestId = 0;
static IOTHUB_ACCOUNT_INFO_HANDLE g_iothubAcctInfo = NULL;

#define IOTHUB_COUNTER_MAX           10
#define IOTHUB_TIMEOUT_SEC           1000
#define MAX_CLOUD_TRAVEL_TIME        60.0

DEFINE_MICROMOCK_ENUM_TO_STRING(IOTHUB_TEST_CLIENT_RESULT, IOTHUB_TEST_CLIENT_RESULT_VALUES);
DEFINE_MICROMOCK_ENUM_TO_STRING(IOTHUB_CLIENT_RESULT, IOTHUB_CLIENT_RESULT_VALUES);
DEFINE_MICROMOCK_ENUM_TO_STRING(MAP_RESULT, MAP_RESULT_VALUES);

typedef struct EXPECTED_SEND_DATA_TAG
{
	const char* expectedString;
	bool wasFoundInHub;
	bool dataWasSent;
	time_t timeSent;
	time_t timeReceived;
	LOCK_HANDLE lock;
} EXPECTED_SEND_DATA;


BEGIN_TEST_SUITE(longhaul_tests)

static int IoTHubCallback(void* context, const char* data, size_t size)
{
	size;
	int result = 0; // 0 means "keep processing"

	EXPECTED_SEND_DATA* expectedData = (EXPECTED_SEND_DATA*)context;
	if (expectedData != NULL)
	{
		if (Lock(expectedData->lock) != LOCK_OK)
		{
			ASSERT_FAIL("unable to lock");
		}
		else
		{
			if (
				(strlen(expectedData->expectedString) == size) &&
				(memcmp(expectedData->expectedString, data, size) == 0)
				)
			{
				expectedData->wasFoundInHub = true;
				expectedData->timeReceived = time(NULL);
				result = 1;
			}
			(void)Unlock(expectedData->lock);
		}
	}
	return result;
}

static void SendConfirmationCallback(IOTHUB_CLIENT_CONFIRMATION_RESULT result, void* userContextCallback)
{
	result;
	EXPECTED_SEND_DATA* expectedData = (EXPECTED_SEND_DATA*)userContextCallback;
	if (expectedData != NULL)
	{
		if (Lock(expectedData->lock) != LOCK_OK)
		{
			ASSERT_FAIL("unable to lock");
		}
		else
		{
			expectedData->dataWasSent = true;
			expectedData->timeSent = time(NULL);
			(void)Unlock(expectedData->lock);
		}
	}
}

static EXPECTED_SEND_DATA* EventData_Create(void)
{
	EXPECTED_SEND_DATA* result = (EXPECTED_SEND_DATA*)malloc(sizeof(EXPECTED_SEND_DATA));
	if (result != NULL)
	{
		if ((result->lock = Lock_Init()) == NULL)
		{
			ASSERT_FAIL("unable to Lock_Init");
		}
		else
		{
			char temp[1000];
			char* tempString;
			time_t t = time(NULL);
			sprintf(temp, TEST_EVENT_DATA_FMT, ctime(&t), g_iotHubTestId);
			if ((tempString = (char*)malloc(strlen(temp) + 1)) == NULL)
			{
				Lock_Deinit(result->lock);
				free(result);
				result = NULL;
			}
			else
			{
				strcpy(tempString, temp);
				result->expectedString = tempString;
				result->wasFoundInHub = false;
				result->dataWasSent = false;
				result->timeSent = 0;
				result->timeReceived = 0;
			}
		}
	}
	return result;
}

static void EventData_Destroy(EXPECTED_SEND_DATA* data)
{
	if (data != NULL)
	{
		(void)Lock_Deinit(data->lock);
		if (data->expectedString != NULL)
		{
			free((void*)data->expectedString);
		}
		free(data);
	}
}

typedef struct LONGHAUL_STATISTICS_TAG
{
	double numberOfEventsReceivedByHub;
	double minEventTravelTime;
	double maxEventTravelTime;
	double avgEventTravelTime;
} LONGHAUL_STATISTICS;

static LONGHAUL_STATISTICS* initializeStatistics()
{
	LONGHAUL_STATISTICS* stats = (LONGHAUL_STATISTICS*)malloc(sizeof(LONGHAUL_STATISTICS));

	if (stats != NULL)
	{
		stats->numberOfEventsReceivedByHub = 0;
		stats->minEventTravelTime = LONG_MAX;
		stats->maxEventTravelTime = 0;
		stats->avgEventTravelTime = 0;
	}

	return stats;
}

static void computeStatistics(LONGHAUL_STATISTICS* stats, EXPECTED_SEND_DATA* data)
{
	if (data->dataWasSent && data->wasFoundInHub)
	{
		double eventTravelTime = difftime(data->timeReceived, data->timeSent);

		if (eventTravelTime > stats->maxEventTravelTime)
		{
			stats->maxEventTravelTime = eventTravelTime;
		}

		if (eventTravelTime < stats->minEventTravelTime)
		{
			stats->minEventTravelTime = eventTravelTime;
		}

		stats->avgEventTravelTime = (stats->avgEventTravelTime * stats->numberOfEventsReceivedByHub + eventTravelTime) / (stats->numberOfEventsReceivedByHub + 1);

		stats->numberOfEventsReceivedByHub++;
	}
}

static void printStatistics(LONGHAUL_STATISTICS* stats)
{
	LogInfo("Number of Events: Sent=%f, Received=%f; Travel Time (secs): Min=%f, Max=%f, Average=%f",
		stats->numberOfEventsReceivedByHub, stats->numberOfEventsReceivedByHub, stats->minEventTravelTime, stats->maxEventTravelTime, stats->avgEventTravelTime);
}

static void destroyStatistics(LONGHAUL_STATISTICS* stats)
{
	free(stats);
}

static int GetLonghaulTestDurationInSeconds(int defaultDurationInSeconds)
{
	int testDuration = 0;

	char *envVar = getenv("IOTHUB_LONGHAUL_TEST_DURATION_IN_SECONDS");

	if (envVar != NULL)
	{
		testDuration = atoi(envVar);
	}

	if (testDuration <= 0)
	{
		testDuration = defaultDurationInSeconds;
	}

	return testDuration;
}

// Description:
//     This function blocks the current thread for the necessary ammount of time to complete 1 cycle of the expectedFrequencyInHz.
//     If (currentTimeInMilliseconds - initialTimeInMilliseconds) is longer than expectedFrequencyInHz, the function exits without blocking. 
// Parameters:
//     expectedFrequencyInHz: value (in Hertz, i.e. number of cycles per second) used to calculate the duration of 1 cycle of the expected frequency (min value = 1). 
//     initialTimeInMilliseconds: initial time of the cycle in milliseconds (as returned by 'clock() * 1000 / CLOCKS_PER_SEC').
// Returns:
//     Nothing.
static void WaitForFrequencyMatch(size_t expectedFrequencyInHz, size_t initialTimeInMilliseconds)
{
	if (expectedFrequencyInHz == 0) expectedFrequencyInHz = 1;

	size_t cycleExpectedTimeInMilliseconds = 1000 / expectedFrequencyInHz;

	size_t currentTimeInMilliseconds = (clock() * 1000) / CLOCKS_PER_SEC;

	if ((currentTimeInMilliseconds - initialTimeInMilliseconds) < cycleExpectedTimeInMilliseconds)
	{
		size_t cycleRemainingTimeInMilliseconds = cycleExpectedTimeInMilliseconds - (currentTimeInMilliseconds - initialTimeInMilliseconds);

		ThreadAPI_Sleep(cycleRemainingTimeInMilliseconds);
	}
}

#ifdef MBED_BUILD_TIMESTAMP
static void verifyEventReceivedByHub(EXPECTED_SEND_DATA* sendData)
{
		(void)printf("VerifyMessageReceived[%s] sent on [%s]\r\n", sendData->expectedString, ctime(&sendData->timeSent));

		int response = -1;
		scanf("%d", &response);

		if (response == 0 || response == 1)
		{
			sendData->wasFoundInHub = response;

			if (response == 0)
			{
				ASSERT_FAIL("Event not received by IoT hub within expected time.\r\n");
			}
		}
		else
		{
			LogError("Failed getting result of verification of Events received by hub.");
		}
}
#else
static void verifyEventReceivedByHub(EXPECTED_SEND_DATA* sendData, IOTHUB_TEST_HANDLE iotHubTestHandle)
{
	IOTHUB_TEST_CLIENT_RESULT result = IoTHubTest_ListenForEventForMaxDrainTime(iotHubTestHandle, IoTHubCallback, IoTHubAccount_GetIoTHubPartitionCount(g_iothubAcctInfo), sendData);
	ASSERT_ARE_EQUAL(IOTHUB_TEST_CLIENT_RESULT, IOTHUB_TEST_CLIENT_OK, result);

	// assert
	ASSERT_IS_TRUE_WITH_MSG(sendData->wasFoundInHub, "Failure verifying if data was received by Event Hub.");
}
#endif

void RunLongHaulTest(int totalRunTimeInSeconds, int eventFrequencyInHz)
{
	LogInfo("Starting Long Haul tests (totalRunTimeInSeconds=%d, eventFrequencyInHz=%d)\r\n", totalRunTimeInSeconds, eventFrequencyInHz);

	// arrange
	IOTHUB_CLIENT_CONFIG iotHubConfig = { 0 };
	IOTHUB_CLIENT_HANDLE iotHubClientHandle;
	IOTHUB_CLIENT_RESULT result;

#ifndef MBED_BUILD_TIMESTAMP
	LONGHAUL_STATISTICS *stats;

	stats = initializeStatistics();
	ASSERT_IS_NOT_NULL_WITH_MSG(stats, "Failed creating the container to track statistics.");
#endif

	iotHubConfig.iotHubName = IoTHubAccount_GetIoTHubName(g_iothubAcctInfo);
	iotHubConfig.iotHubSuffix = IoTHubAccount_GetIoTHubSuffix(g_iothubAcctInfo);
	iotHubConfig.deviceId = IoTHubAccount_GetDeviceId(g_iothubAcctInfo);
	iotHubConfig.deviceKey = IoTHubAccount_GetDeviceKey(g_iothubAcctInfo);
	iotHubConfig.protocol = AMQP_Protocol;

	// Create the IoT Hub Data
#ifndef MBED_BUILD_TIMESTAMP
	IOTHUB_TEST_HANDLE iotHubTestHandle = IoTHubTest_Initialize(IoTHubAccount_GetEventHubConnectionString(g_iothubAcctInfo), IoTHubAccount_GetIoTHubConnString(g_iothubAcctInfo), IoTHubAccount_GetDeviceId(g_iothubAcctInfo), IoTHubAccount_GetDeviceKey(g_iothubAcctInfo), IoTHubAccount_GetEventhubListenName(g_iothubAcctInfo), IoTHubAccount_GetEventhubAccessKey(g_iothubAcctInfo), IoTHubAccount_GetSharedAccessSignature(g_iothubAcctInfo), IoTHubAccount_GetEventhubConsumerGroup(g_iothubAcctInfo));
	ASSERT_IS_NOT_NULL_WITH_MSG(iotHubTestHandle, "Failed initializing the Event Hub listener.");
#endif

	iotHubClientHandle = IoTHubClient_Create(&iotHubConfig);
	ASSERT_IS_NOT_NULL_WITH_MSG(iotHubClientHandle, "Failed creating the IoT Hub Client.");

#ifdef MBED_BUILD_TIMESTAMP
	result = IoTHubClient_SetOption(iotHubClientHandle, "TrustedCerts", certificates);
	ASSERT_ARE_EQUAL_WITH_MSG(int, IOTHUB_CLIENT_OK, result, "Failed setting certificates on IoT Hub client.");
#endif

	time_t testInitialTime = time(NULL);

	while (difftime(time(NULL), testInitialTime) <= totalRunTimeInSeconds)
	{
		size_t loopInitialTimeInMilliseconds;
		EXPECTED_SEND_DATA* sendData;
		IOTHUB_MESSAGE_HANDLE msgHandle;

		loopInitialTimeInMilliseconds = (clock() * 1000) / CLOCKS_PER_SEC;

		sendData = EventData_Create();
		ASSERT_IS_NOT_NULL_WITH_MSG(sendData, "Failed creating EXPECTED_SEND_DATA.");

		// act
		msgHandle = IoTHubMessage_CreateFromByteArray((const unsigned char*)sendData->expectedString, strlen(sendData->expectedString));
		ASSERT_IS_NOT_NULL_WITH_MSG(msgHandle, "Failed creating IOTHUB_MESSAGE_HANDLE.");

		// act
		result = IoTHubClient_SendEventAsync(iotHubClientHandle, msgHandle, SendConfirmationCallback, sendData);
		ASSERT_ARE_EQUAL_WITH_MSG(int, IOTHUB_CLIENT_OK, result, "Call to IoTHubClient_SendEventAsync failed.");

		time_t beginOperation, nowTime;
		beginOperation = time(NULL);
		while (
			(nowTime = time(NULL)),
			(difftime(nowTime, beginOperation) < MAX_CLOUD_TRAVEL_TIME) // time box
			)
		{
			if (Lock(sendData->lock) != LOCK_OK)
			{
				ASSERT_FAIL("unable to lock");
			}
			else
			{
				if (sendData->dataWasSent)
				{
					Unlock(sendData->lock);
					break;
				}
				Unlock(sendData->lock);
			}
			ThreadAPI_Sleep(100);
		}

		if (Lock(sendData->lock) != LOCK_OK)
		{
			ASSERT_FAIL("unable to lock");
		}
		else
		{
			ASSERT_IS_TRUE_WITH_MSG(sendData->dataWasSent, "Failure sending data to IotHub");
			(void)Unlock(sendData->lock);
		}

#ifdef MBED_BUILD_TIMESTAMP
		verifyEventReceivedByHub(sendData);
#else
		verifyEventReceivedByHub(sendData, iotHubTestHandle);
		computeStatistics(stats, sendData);
#endif

		IoTHubMessage_Destroy(msgHandle);
		EventData_Destroy(sendData);

		WaitForFrequencyMatch(eventFrequencyInHz, loopInitialTimeInMilliseconds);
	}

	// cleanup
	IoTHubClient_Destroy(iotHubClientHandle);

#ifndef MBED_BUILD_TIMESTAMP
	IoTHubTest_Deinit(iotHubTestHandle);
	
	printStatistics(stats);
	destroyStatistics(stats);
#endif

	LogInfo("Long Haul tests completed\r\n");
}


TEST_SUITE_INITIALIZE(TestClassInitialize)
{
	ASSERT_ARE_EQUAL(int, 0, platform_init());
	INITIALIZE_MEMORY_DEBUG(g_dllByDll);
	platform_init();
	g_iothubAcctInfo = IoTHubAccount_Init(true, "longhaul_tests");
	ASSERT_IS_NOT_NULL(g_iothubAcctInfo);
	platform_init();
}

TEST_SUITE_CLEANUP(TestClassCleanup)
{
	IoTHubAccount_deinit(g_iothubAcctInfo);
	// Need a double deinit
	platform_deinit();
	DEINITIALIZE_MEMORY_DEBUG(g_dllByDll);
	platform_deinit();
}

TEST_FUNCTION_INITIALIZE(TestMethodInitialize)
{
	g_iotHubTestId++;
}

TEST_FUNCTION_CLEANUP(TestMethodCleanup)
{
}

TEST_FUNCTION(IoTHubClient_LongHaul_12h_Run_1_Event_Per_Sec)
{
	const int TEST_MAX_TIME_IN_SECONDS = 12 * 60 * 60;
	const int EVENT_FREQUENCY_IN_HZ = 1;

	int testDuration = GetLonghaulTestDurationInSeconds(TEST_MAX_TIME_IN_SECONDS);
	
	RunLongHaulTest(testDuration, EVENT_FREQUENCY_IN_HZ);
}

END_TEST_SUITE(longhaul_tests)

