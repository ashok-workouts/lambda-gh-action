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

import * as _ from "lodash";

const client = new S3Client({ region: "us-east-1" });
const file_name = "Sample_xml_processingfile.xml"
const bucket = "ak-workouts-us-east-1"

async function getObject(key: string): Promise<GetObjectCommandOutput> {
  // const appId = `${svcName} | getObject`;
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

async function getObjectAsString(key: string): Promise<string> {
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


export const handler: SQSHandler = async (event: SQSEvent) => {
  
  try {
      const body = getObjectAsString(file_name);
      console.log(`The file body is:: ${body}`)
  }
  catch (err) {
      console.log(err);
      throw err;

  }
};