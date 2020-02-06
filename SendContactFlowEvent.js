const aws = require("aws-sdk");
const kinesis = new aws.Kinesis();

exports.handler = async (event) => {
    return await kinesis.putRecord(
        {
            Data: JSON.stringify(event),
            PartitionKey: "SendContactFlowEventKey", // シャード数が1個固定なら固定値で問題ありません。
            StreamName: process.env.STREAM_NAME,
        }).promise();
};