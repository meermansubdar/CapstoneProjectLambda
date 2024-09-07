const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const dynamodb = new AWS.DynamoDB.DocumentClient();

exports.handler = async (event) => {
    console.log('Received event:', JSON.stringify(event, null, 2));

    for (const record of event.Records) {
        console.log('Processing record:', JSON.stringify(record, null, 2));

        const snsMessage = JSON.parse(record.Sns.Message);
        const bucket = snsMessage.detail.bucket.name;
        const key = snsMessage.detail.object.key;

        try {
            const s3Object = await s3.getObject({ Bucket: bucket, Key: key }).promise();
            const rawData = s3Object.Body.toString('utf-8');
            console.log('Raw S3 object data:', rawData);

            const jsonObjects = rawData.split('}{').map((item, index, array) => {
                if (index === 0) return item + '}';
                if (index === array.length - 1) return '{' + item;
                return '{' + item + '}';
            });

            // Process each JSON object in parallel
            const promises = jsonObjects.map(async (jsonObject) => {
                let data;
                try {
                    data = JSON.parse(jsonObject);
                } catch (parseError) {
                    console.error('Error parsing JSON data:', parseError);
                    return;
                }

                const { vehicleId, vehicleMake, vehicleSpeed, vehicleLongitude, vehicleLatitude } = data;

                if (vehicleId && vehicleMake && vehicleSpeed && vehicleLongitude && vehicleLatitude) {
                    const dbParams = {
                        TableName: 'CapstoneDynamoDBTable',
                        Item: {
                            vehicleId,
                            vehicleMake,
                            vehicleSpeed,
                            vehicleLongitude,
                            vehicleLatitude
                        }
                    };

                    try {
                        await dynamodb.put(dbParams).promise();
                        console.log('Data stored in DynamoDB:', JSON.stringify(dbParams.Item));
                    } catch (err) {
                        console.error('Error storing data in DynamoDB:', err);
                    }
                } else {
                    console.log('Ignoring blank or incomplete data:', JSON.stringify(data));
                }
            });

            await Promise.all(promises);
        } catch (err) {
            console.error('Error getting object from S3:', err);
        }
    }
};

