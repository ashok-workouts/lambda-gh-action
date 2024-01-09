// import { APIGatewayProxyEvent, APIGatewayProxyResultV2, Handler } from 'aws-lambda';
// import * as _ from 'lodash';

// export const handler: Handler = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResultV2> => {
//   const max = 200;
//   const val = _.random(max);
//   const response = {
//     statusCode: 200,
//     body: `The random value (max ${max}) is: ${val}`,
//   };
//   return response;
// };

// import { SQSService } from 'sqs';
import { promises as fsPromises } from 'fs'; 
import { parseString } from 'xml2js';

import { 
	SQSEvent, 
	SQSHandler
  } from 'aws-lambda';
  
import {
	S3Client,
	GetObjectCommandInput,
	GetObjectCommand,
	GetObjectCommandOutput
  } from '@aws-sdk/client-s3';
  
import xmlJs from 'xml-js';
import * as _ from "lodash";
  
const client = new S3Client({ region: "us-east-1" });
// const file_name = "Sample_xml_processingfile.xml"
const file_name = "Sample_945ECom_E102.xml"
const bucket = "ak-workouts-us-east-1"
  
async function getObject(key: string): Promise<GetObjectCommandOutput> {
	console.log("getObject() started execution........")
	const params: GetObjectCommandInput = {
		Bucket: bucket,
		Key: file_name,
	};
	const command = new GetObjectCommand(params);
  
	try {
		return await client.send(command);
	} catch (error) {
		console.log(error.message, { bucket, key }, error);
		throw error;
	}
}
  

// async function getObjectAsString(key: string): Promise<string> {
async function getObjectAsString(key: string) {
	console.log("getObjectAsString() started execution........")
	try {
		const s3Object = await getObject(key);
  
		if (!s3Object.Body) {
			throw new Error(`No objectstring found for ${key}, ${bucket}`);
		}
  
		return await s3Object.Body.transformToString();
	} catch (error) {
	  console.log(error.message, { key, bucket }, error);
		throw error;
	}
}
  

export function parseWebStore(webStore: string): number {
	console.log("parseWebStore() started execution........")
	return parseInt(webStore.replace(/\D/g, ''), 10);
}
  
  
export function getFirstTextXml(item: any): string {
	console.log("getFirstTextXml() started execution........")
	if (!item || !item[0] || !item[0]._text || !item[0]._text[0]) return '';
	return item[0]._text[0].trim();
}

export interface PackageLineItem {
	SKU: string;
	UPC?: string;
	quantity: number;
}


export interface BaseFulfillmentData {
	orderId: number;
	shipDate: string;
	shippingName: string;
	fulfillEntireOrder?: boolean;
	packages: {
		packageShipDate: string;
		trackingNumber: string;
		lines: PackageLineItem[];
	}[];
}


export interface CsvFulfillmentData extends BaseFulfillmentData {
	phoneNumber: string;
	name: string;
}


export type FulfillmentData = BaseFulfillmentData | CsvFulfillmentData;


export interface ParsedFulfillmentData {
	webStore: number;
	fulfillments: FulfillmentData[];
}


export interface FullfillmentXmlData {
	[Data945: string]: [
		{
			WebStore: [{ _text: string }];
			documents: [
				{
					document: [
						{
							headerrow: [
								{
									reference: [{ _text: string }];
									shipdate: [{ _text: string }];
									transportationcode: [{ _text: string }];
								},
							];
							documentpackages: [
								{
									packagerow: [
										{
											packagenumber: [{ _text: string }];
											package_shipdate: [{ _text: string }];
											document_lines: [
												{
													linerow: [
														{
															SKU: [{ _text: string }];
															UPC: [{ _text: string }];
															quantity: [{ _text: string }];
														},
													];
												},
											];
										},
									];
								},
							];
						},
					];
				},
			];
		},
	];
}

  
export function parseFulfillmentXml(xml: string, filename: string): ParsedFulfillmentData {
	console.log("parseFulfillmentXml() started execution........")
	const appId = 'parseFulfillmentXml';
	try {
		console.log("In try block of parseFulfillmentXml()....")
		// const json = xml2Js.xml2js(xml, {
		// 	compact: true,
		// 	alwaysArray: true,
		// });
		const json = xmlJs.xml2js(xml, {
			compact: true,
			alwaysArray: true,
		}) as unknown as FullfillmentXmlData;
		console.log("json is: ", json)

		// this should never happen
		if (!json?.Data945) {
			throw new Error(`parseFulfillmentXml: malformed xml file ${filename}`);
		}

		return {
			webStore: parseWebStore(getFirstTextXml(json.Data945[0].WebStore)),
			// we need to drill down a few levels to get to the '<document>' node
			fulfillments: json.Data945[0].documents[0].document.map(document => {
				const header = document.headerrow[0];
				return {
					orderId: parseInt(getFirstTextXml(header?.reference), 10),
					shipDate: getFirstTextXml(header?.shipdate),
					shippingName: getFirstTextXml(header?.transportationcode),
					// get each package with in the document
					packages: document?.documentpackages[0].packagerow.map(packageRow => {
						return {
							trackingNumber: getFirstTextXml(packageRow?.packagenumber),
							packageShipDate: getFirstTextXml(packageRow?.package_shipdate),
							lines: packageRow?.document_lines[0]?.linerow.map(line => {
								return {
									SKU: getFirstTextXml(line.SKU),
									UPC: getFirstTextXml(line.UPC),
									quantity: parseInt(getFirstTextXml(line.quantity), 10),
								};
							}),
						};
					}),
				};
			}),
		};
	} catch (error) {
		console.log(appId, `${filename} ${error.message}`, { filename }, error);
		throw error;
	}
}


export async function processFulfillmentJson(
	webStore: number,
	fulfillments: FulfillmentData[],
	filename: string,
): Promise<void> {
	const appId = `processFulfillmentJson`;
	const FULFILLMENT_ITEM_QUEUE = "FULFILLMENT_ITEM_QUEUE";
	console.log("Started execution processFulfillmentJson()...........")
	try {
		if (!fulfillments || fulfillments?.length < 1) {
			throw new Error(filename);
		}

		// --- break up fulfillments into units and send to SQS
		// --- break up packages by package row to ease fulfillment
		const sqsMessages = fulfillments.flatMap(fulfillment => {
			return fulfillment.packages.map(packageRow => {
				const payload: {
					webStore: number;
					filename: string;
					data: FulfillmentData;
				} = {
					webStore,
					filename,
					data: {
						orderId: fulfillment.orderId,
						shipDate: fulfillment.shipDate,
						shippingName: fulfillment.shippingName,
						phoneNumber: 'phoneNumber' in fulfillment ? fulfillment.phoneNumber : undefined,
						name: 'name' in fulfillment ? fulfillment.name : undefined,
						fulfillEntireOrder: fulfillment.fulfillEntireOrder,
						packages: [packageRow],
					},
				};

				return JSON.stringify(payload);
			});
		});
		console.log("sqsMessages is: ", sqsMessages);
		// await SQSService(FULFILLMENT_ITEM_QUEUE).chunkAndPublish(sqsMessages);
	} catch (error) {
		// const storeUrl = getAdminUrl(webStore);
		// const logParams = { webStore, storeUrl, filename };
		throw error()
		console.log(error.message, error);
		throw error;
	}
}


export const handler: SQSHandler = async (event: SQSEvent) => {
	console.log("Handler started execution........")
	try {
		const xmlbody = await getObjectAsString(file_name);
		console.log(`The xml file body is:: ${xmlbody}`)
		const { fulfillments, webStore: newWebStore } = parseFulfillmentXml(xmlbody, file_name);
		console.log("fulfillments is: ", fulfillments)
		console.log("webStore is: ", newWebStore)
		await processFulfillmentJson(newWebStore, fulfillments, file_name);
	}
	catch (err) {
		console.log(err);
		throw err;
  
	}
};