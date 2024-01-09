import {
	MessageAttributeValue,
	SendMessageBatchCommand,
	SendMessageBatchCommandInput,
	SendMessageCommand,
	SendMessageCommandInput,
	SQSClient,
} from '@aws-sdk/client-sqs';
// import { MissingEnvVarError, MissingQueueArnError } from '@siberia-libs/helpers/customErrors';
// import log from '@siberia-libs/helpers/log';
import PQueue from 'p-queue';
import { v4 as uuidv4 } from 'uuid';

export interface SendParams {
	message: string;
	messageAttributes?: Record<string, MessageAttributeValue>;
	id?: string;
	delaySeconds?: number;
	messageDeduplicationId?: string;
	messageGroupId?: string;
}

interface SQSServiceResponse {
	publish: (publishParams: SendParams) => Promise<void>;
	publishBatch: (messages: string[]) => Promise<void>;
	chunkAndPublish: (messages: string[]) => Promise<void>;
}

const AWS_REGION = 'us-east-1';
const _sqsClient = new SQSClient({ region: AWS_REGION });

// eslint-disable-next-line sonarjs/cognitive-complexity
export function SQSService(queueArn?: string): SQSServiceResponse {
	const svcName = `🗼 SQSService`;

	if (!AWS_REGION) {
		// Should never happen as this is a standard Lambda env var
		throw new Error('AWS_REGION');
	}
	const CHUNK_SIZE = 10;
	const PROMISE_QUEUE = new PQueue({ concurrency: 500 });

	const _sqsEndpointUrl = _sqsClient.config.endpointProvider({ Region: AWS_REGION }).url.href;

	function _deconstructQueueArn() {
		if (!queueArn) {
			throw new Error();
		}
		const accountId = queueArn.split(':')[4];
		const queueName = queueArn.split(':')[5];
		return `${_sqsEndpointUrl}${accountId}/${queueName}`;
	}

	async function publish(publishParams: SendParams) {
		const appId = `${svcName} | publish`;
		const queueUrl = _deconstructQueueArn();
		const {
			message,
			messageAttributes,
			id,
			delaySeconds,
			messageDeduplicationId,
			messageGroupId,
		} = publishParams;
		const logParams = { id, queueUrl, message, messageAttributes, queueArn };

		const params: SendMessageCommandInput = {
			QueueUrl: queueUrl,
			MessageBody: message,
		};

		if (messageAttributes) {
			params.MessageAttributes = messageAttributes;
		}
		if (delaySeconds) {
			params.DelaySeconds = Math.round(delaySeconds);
		}
		if (messageDeduplicationId) {
			params.MessageDeduplicationId = messageDeduplicationId;
		}
		if (messageGroupId) {
			params.MessageGroupId = messageGroupId;
		}
		try {
			const command = new SendMessageCommand(params);
			await _sqsClient.send(command);
		} catch (error) {
			console.log(appId, error.message, logParams, error);
			throw error;
		}
	}

	async function publishBatch(messages: string[]) {
		const appId = `${svcName} | publishBatch`;
		const queueUrl = _deconstructQueueArn();
		const params: SendMessageBatchCommandInput = {
			QueueUrl: queueUrl,
			Entries: [],
		};

		for (const message of messages) {
			params.Entries?.push({
				Id: uuidv4(),
				MessageBody: message,
			});
		}

		try {
			const command = new SendMessageBatchCommand(params);
			const result = await _sqsClient.send(command);
			console.log(appId, 'SQS Result', result);
		} catch (error) {
			console.log(appId, error.message, {}, error);
			throw error;
		}
	}

	async function chunkAndPublish(messages: string[]) {
		const chunks: string[][] = [];

		let i, j, temparray;
		for (i = 0, j = messages.length; i < j; i += CHUNK_SIZE) {
			temparray = messages.slice(i, i + CHUNK_SIZE);
			chunks.push(temparray);
		}
		for (const chunkitems of chunks) {
			PROMISE_QUEUE.add(async () => {
				await publishBatch(chunkitems);
			});
		}

		await PROMISE_QUEUE.onIdle();
	}

	return { publish, publishBatch, chunkAndPublish };
}
