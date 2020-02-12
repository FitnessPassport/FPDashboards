var AWS = require('aws-sdk');
var https = require('https');
var fs = require('fs');
var momentTimezone = require('moment-timezone');

var ses = new AWS.SES({ apiVersion: '2010-12-01' });
var sqs = new AWS.SQS({ region: process.env.Region, httpOptions: { agent: agent } });
var s3 = new AWS.S3();
var agent = new https.Agent({ maxSockets: 150 });

var queueURL = process.env.QueueURL;
var toaddresses = process.env.ToAddr;
var bccaddresses = process.env.BccAddr;
var bccaddresses_1 = process.env.BccAddr1;
var bccaddresses_2 = process.env.BccAddr2;
var srcaddr = process.env.SrcAddr;
var bucket = process.env.BucketName;
var prefix = process.env.BucketPrefix;
var qSize = null;
var content = null;
var queueParams = {AttributeNames: ["ApproximateNumberOfMessages"], QueueUrl: queueURL};


exports.handler = (event, context, callback) => {
    // Convert date to local timezone
    var date = momentTimezone.tz((new Date()).toString(),'Australia/Sydney').format('MMM-DD-YYYY-HH-mm-ss');
    console.log('Date for file name: ', date);

    var url = null;

    function s3upload() {
        if (prefix == undefined) {
            prefix = "";
        }
        var param = {
            Bucket: bucket,
            Key: prefix + date + ".html",
            Body: content,
            ACL: 'public-read',
            ContentType: "text/html"
        };
        s3.upload(param, function (err, data) {
            if (err) console.log(err, err.stack); // an error occurred
            else console.log(data);
            url = data.Location;
            console.log("uploading to s3");
            if (toaddresses) {
                sendMail();
            }
            //context.done();
        });
    }

    function sendMail() {
        //console.log("message: " +  messages);
        var params = {
            Destination: {
                ToAddresses: [toaddresses, bccaddresses, bccaddresses_1, bccaddresses_2]
            },
            Message: {
                Body: {
                    Html: {
                        Data: url,
                        Charset: 'utf-8'
                    },
                    Text: {
                        Data: "report",
                        Charset: 'utf-8'
                    }
                },
                Subject: {
                    Data: "FPUAT [SES] Daily -  Notification Reports",
                    Charset: 'utf-8'
                }
            },
            Source: srcaddr,
        };
        ses.sendEmail(params, function (err, data) {
            if (err) console.log(err, err.stack);
            else console.log(data);
            console.log("sending email");
            context.done();
        });
    }

    function initializeQueue(callbackQueue) {
        console.log("Reading from: " + queueURL);
        sqs.getQueueAttributes(queueParams, (err, data) => {
            if (err) {
                console.log("Possible issue with SQS permissions or QueueURL wrong")
                callbackQueue(err, null);
            } 
            qSize = data.Attributes.ApproximateNumberOfMessages;
            callbackQueue(null, qSize);
        });
    }

    function deleteMessage(message) {
        sqs.deleteMessage({
            QueueUrl: queueURL,
            ReceiptHandle: message.ReceiptHandle
        }, (err, data) => {
            if (err) {
                console.log(err);
                throw err;
            }
            // console.log("Data removed. Response = " + data);  
        });
    }

    //Start Receive message
    initializeQueue((err, queueSize) => {
        console.log("Reading queue, size = " + queueSize);

        if (queueSize == 0) {
            callback(null, 'Queue is empty.');
        }

        var messages = [];
        var msgBouncePerm = [];
        var msgSuppres = [];
        var msgBounceTrans = [];
        var msgComplaint = [];
        var msgDelivered = [];

        for (var i = 0; i < queueSize; i++) {
            sqs.receiveMessage(queueParams, (err, data) => {
                if (err) {
                    console.log(err, err.stack);
                    throw err;
                }

                console.log("data with message = " + data.Messages);
                if (data.Messages) {
                    var message = data.Messages[0];
                    body = JSON.parse(message.Body);
                    msg = JSON.parse(body.Message);
                    var source = msg.mail.source;
                    var subject = msg.mail.commonHeaders.subject;
                    var type = msg.notificationType;
                    
                    //Convert timestamp to local time
                    var time = msg.mail.timestamp;
                    time = momentTimezone.tz(time.toString(), 'Australia/Sydney');
                    time = time.format('YYYY-MM-DD HH:mm:ss');
                    console.log('Message Time stamp: ', time);

                    var source_ip = msg.mail.sourceIp;
                    var id = msg.mail.messageId;
                    var otr = "<tr>";
                    var ftr = "</tr>";
                    var oline = "<td>";
                    var cline = "</td>";
                    var btype = null;
                    var bsubtype = null;
                    var diagcode = null;
                    console.log(msg);


                    if (type == "Bounce") {
                        var destination = msg.bounce.bouncedRecipients[0].emailAddress;
                        btype = msg.bounce.bounceType; // Permanent || Transient
                        bsubtype = msg.bounce.bounceSubType; // General || Supressed
                        if (btype == "Permanent" && bsubtype == "Suppressed") {
                            diagcode = "Suppressed by SES";
                            var text = otr + oline + type + cline + oline + btype + cline + oline + " Subject: "+ subject + cline + oline + source + cline + oline + destination  + cline + oline + diagcode + cline + oline + source_ip + cline + oline + time + cline + oline + id + cline + ftr;
                            msgSuppres.push(text);

                        } else if (btype == "Permanent" && bsubtype == "General") {
                            diagcode = msg.bounce.bouncedRecipients[0].diagnosticCode;
                            var text = otr + oline + type + cline + oline + btype + cline + oline + " Subject: "+ subject + cline + oline + source + cline + oline + destination  + cline + oline + diagcode + cline + oline + source_ip + cline + oline + time + cline + oline + id + cline + ftr;
                            msgBouncePerm.push(text);

                        } else if (btype == "Permanent" && bsubtype == "NoEmail") {
                            diagcode = msg.bounce.bouncedRecipients[0].diagnosticCode;
                            var text = otr + oline + type + cline + oline + btype + cline + oline + " Subject: "+ subject + cline + oline + source + cline + oline + destination + cline + oline + diagcode + cline + oline + source_ip + cline + oline + time + cline + oline + id + cline + ftr;
                            msgBouncePerm.push(text);

                        } else if (btype == "Undetermined") {
                            diagcode = msg.bounce.bouncedRecipients[0].diagnosticCode;
                            var text = otr + oline + type + cline + oline + btype + cline + oline + " Subject: "+ subject + cline + oline + source + cline + oline + destination  + cline + oline + diagcode + cline + oline + source_ip + cline + oline + time + cline + oline + id + cline + ftr;
                            msgBouncePerm.push(text);

                        } else if (btype == "Transient") {
                            diagcode = "soft-Bounce";
                            var text = otr + oline + type + cline + oline + btype + cline + oline + " Subject: "+ subject + cline + oline + source + cline + oline + destination  + cline + oline + diagcode + cline + oline + source_ip + cline + oline + time + cline + oline + id + cline + ftr;
                            msgBounceTrans.push(text);

                        } else {
                            console.log("it's an unknown bounce");
                            diagcode = "unknown";
                            var text = otr + oline + type + cline + oline + btype + cline + oline + " Subject: "+ subject + cline + oline + source + cline + oline + destination  + cline + oline + diagcode + cline + oline + source_ip + cline + oline + time + cline + oline + id + cline + ftr;
                            msgBouncePerm.push(text);
                        }

                    } else if (type == "Delivery") {
                        console.log("Delivery notification not supported");
                        var destination = msg.delivery.recipients[0];
                        btype = "null";
                        bsubtype = "null";
                        diagcode = "null";
                        var text = otr + oline + type + cline + oline + btype + cline + oline + " Subject: "+ subject + cline + oline + source + cline + oline + destination  + cline + oline + diagcode + cline + oline + source_ip + cline + oline + time + cline + oline + id + cline + ftr;
                        msgDelivered.push(text);
                        console.log(destination);

                    } else if (type == "Complaint") {
                        var destination = msg.complaint.complainedRecipients[0].emailAddress;
                        btype = "null";
                        bsubtype = "null";
                        diagcode = "null";
                        var text = otr + oline + type + cline + oline + btype + cline + oline + " Subject: "+ subject + cline + oline + source + cline + oline + destination + cline + oline + diagcode + cline + oline + source_ip + cline + oline + time + cline + oline + id + cline + ftr;
                        msgComplaint.push(text);

                    }
                    
                    else {
                        console.log("not identified");
                    }

                    messages.push(i);

                    deleteMessage(message);
                    //console.log("Array size = " + messages.length + " with queue size = " + queueSize);

                    if (messages.length == queueSize) {

                        var bp = msgBouncePerm.join('');
                        var sp = msgSuppres.join('');
                        var bt = msgBounceTrans.join('');
                        var cp = msgComplaint.join('');
                        var dl = msgDelivered.join('');
                        var begin = fs.readFileSync('template/begin_new.html', 'utf8');
                        var middle = bp + sp + bt + cp + dl;
                        var end = fs.readFileSync('template/end_new.html', 'utf8');
                        content = begin + middle + end;

                        s3upload();

                    }
                } else {
                    console.log("data without messages.");
                }
            });
        }
    });
};
