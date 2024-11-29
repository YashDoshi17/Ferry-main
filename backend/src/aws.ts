import { S3 } from "aws-sdk";
import fs from "fs";
import path from "path";
import { Readable } from "stream"; // Import the Readable type

const s3 = new S3({
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    endpoint: process.env.S3_ENDPOINT
});

export const fetchS3Folder = async (key: string, localPath: string): Promise<void> => {
    try {
        const params = {
            Bucket: process.env.S3_BUCKET || "",
            Prefix: key
        };

        const response = await s3.listObjectsV2(params).promise();
        if (response.Contents) {
            await Promise.all(response.Contents.map(async (file) => {
                const fileKey = file.Key;
                if (fileKey) {
                    const getObjectParams = {
                        Bucket: process.env.S3_BUCKET || "",
                        Key: fileKey
                    };

                    const data = await s3.getObject(getObjectParams).promise();
                    if (data.Body) {
                        let fileData: Buffer;

                        if (Buffer.isBuffer(data.Body)) {
                            // If Body is already a Buffer, use it directly
                            fileData = data.Body;
                        } else if (typeof data.Body === 'string') {
                            // If Body is a string, convert it to a Buffer
                            fileData = Buffer.from(data.Body);
                        } else if (data.Body instanceof Uint8Array) {
                            // If Body is a Uint8Array, convert it to a Buffer
                            fileData = Buffer.from(data.Body);
                        } else if (data.Body instanceof Readable) {
                            // If Body is a Readable stream, convert it to a Buffer
                            fileData = await streamToBuffer(data.Body);
                        } else if (data.Body instanceof Blob) {
                            // If Body is a Blob, convert it to a Buffer
                            fileData = await blobToBuffer(data.Body);
                        } else {
                            throw new Error("Unsupported data type in S3 response Body.");
                        }

                        const filePath = `${localPath}/${fileKey.replace(key, "")}`;
                        await writeFile(filePath, fileData);

                        console.log(`Downloaded ${fileKey} to ${filePath}`);
                    }
                }
            }));
        }
    } catch (error) {
        console.error('Error fetching folder:', error);
    }
};

// Convert a Readable stream to a Buffer
function streamToBuffer(stream: Readable): Promise<Buffer> {
    return new Promise((resolve, reject) => {
        const chunks: Buffer[] = [];
        stream.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
        stream.on('end', () => resolve(Buffer.concat(chunks)));
        stream.on('error', reject);
    });
}

// Convert a Blob to a Buffer
function blobToBuffer(blob: Blob): Promise<Buffer> {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onloadend = () => {
            const arrayBuffer = reader.result as ArrayBuffer;
            resolve(Buffer.from(arrayBuffer));
        };
        reader.onerror = reject;
        reader.readAsArrayBuffer(blob);
    });
}

export async function copyS3Folder(sourcePrefix: string, destinationPrefix: string, continuationToken?: string): Promise<void> {
        try {
            // List all objects in the source folder
            const listParams = {
                Bucket: process.env.S3_BUCKET || "",
                Prefix: sourcePrefix,
                ContinuationToken: continuationToken
            };
    
            const listedObjects = await s3.listObjectsV2(listParams).promise();
    
            if (!listedObjects.Contents || listedObjects.Contents.length === 0) return;
            
            // Copy each object to the new location
            await Promise.all(listedObjects.Contents.map(async (object) => {
                if (!object.Key) return;
                let destinationKey = object.Key.replace(sourcePrefix, destinationPrefix);
                let copyParams = {
                    Bucket: process.env.S3_BUCKET || "",
                    CopySource: `${process.env.S3_BUCKET}/${object.Key}`,
                    Key: destinationKey
                };
    
                console.log(copyParams);
    
                await s3.copyObject(copyParams).promise();
                console.log(`Copied ${object.Key} to ${destinationKey}`);
            }));
    
            // Check if the list was truncated and continue copying if necessary
            if (listedObjects.IsTruncated) {
                listParams.ContinuationToken = listedObjects.NextContinuationToken;
                await copyS3Folder(sourcePrefix, destinationPrefix, continuationToken);
            }
        } catch (error) {
            console.error('Error copying folder:', error);
        }
    }

function writeFile(filePath: string, fileData: Buffer): Promise<void> {
    return new Promise(async (resolve, reject) => {
        await createFolder(path.dirname(filePath));

        fs.writeFile(filePath, fileData, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
}

function createFolder(dirName: string) {
    return new Promise<void>((resolve, reject) => {
        fs.mkdir(dirName, { recursive: true }, (err) => {
            if (err) {
                return reject(err);
            }
            resolve();
        });
    });
}

export const saveToS3 = async (key: string, filePath: string, content: string): Promise<void> => {
    const params = {
        Bucket: process.env.S3_BUCKET || "",
        Key: `${key}${filePath}`,
        Body: content
    }

    await s3.putObject(params).promise()
}
