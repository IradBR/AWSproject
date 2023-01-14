import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.util.Base64;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class AWS {
    private static final int MAX_INSTANCES = 19;
    private final S3Client S3;
    private final SqsClient SQS;
    private final Ec2Client EC2;

    public final String managerJarKey = "Manager";
    public final String workerJarKey = "Worker";

    public final String managerJarName = "ManagerProject.jar";
    public final String workerJarName = "WorkerProject.jar";

    public final String bucketName = "assignment1-aws100";
    public static final String ami = "ami-00e95a9222311e8ed";

    public final String localAppSQSName = "LocalAppSQS";
    public final String managerSQSName = "ManagerSQS";
    public final String workerSQSName = "WorkerSQS";

    // new task message - from the application to the manager (location of an input file)
    // new image task message - from the manager to the workers (URL of a specific image)
    // done OCR message - from a worker to the manager (URL of image and identified text)
    // done message - from the manager to the application (S3 location of the output file)
    public enum MessageType {
        NewTask,
        ImageTask,
        DoneOCR,
        DoneMessage,
        Terminate,
        IsManagerAlive
    }

    public final String delimiter = "#";


    private static final AWS instance = new AWS();

    private AWS() {
        EC2 = Ec2Client.builder().region(Region.US_EAST_1).build();
        S3 = S3Client.builder().region(Region.US_WEST_2).build();
        SQS = SqsClient.builder().region(Region.US_WEST_2).build();
    }

    public static AWS getInstance() {
        return instance;
    }

    public Collection<String> createInstance(String script, String tagName, int numberOfInstances) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.M4_LARGE)
                .imageId(ami)
                .maxCount(numberOfInstances)
                .minCount(1)
                .keyName("vockey")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString((script).getBytes()))
                .build();

        RunInstancesResponse response = EC2.runInstances(runRequest);
        Collection<String> instancesIDs = response.instances().stream().map(Instance::instanceId).collect(Collectors.toList());

        Tag tag = Tag.builder()
                .key("Name")
                .value(tagName)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instancesIDs)
                .tags(tag)
                .build();

        try {
            EC2.createTags(tagRequest);
            //System.out.printf("[DEBUG] Successfully started EC2 instance %s based on AMI %s\n", instanceId, ami);

        } catch (Ec2Exception e) {
            System.out.println("[ERROR] " + e.getMessage());
            System.exit(1);
        }

        return instancesIDs;
    }

    //bucket in S3
    public void createBucket(String bucketName) {
        try {
            S3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());
            S3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
        } catch (S3Exception e) {
        }
    }

    public void uploadFileToS3(String bucketName, String keyName, String filePath) {
        S3.putObject(PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(keyName)
                        .build(),
                RequestBody.fromFile(new File(filePath)));
    }

    public void createSQS(String queueName) {
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        SQS.createQueue(createQueueRequest);
    }

    public boolean checkIfInstanceExist(String tagName) {
        return numOfInstance(tagName) > 0;
    }

    public int numOfInstance(String tagName){
        String nextToken = null;
        int counter = 0;

        do {
            DescribeInstancesRequest request = DescribeInstancesRequest
                    .builder().nextToken(nextToken).build();
            DescribeInstancesResponse response = EC2.describeInstances(request);

            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    if (instance.state().name().toString().equals("running")) {
                        for (Tag tag : instance.tags()) {
                            if (tag.key().equals("Name") && tag.value().equals(tagName)) {
                                counter++;
                            }
                        }
                    }
                }
            }
            nextToken = response.nextToken();

        } while (nextToken != null);

        return counter;
    }

    public void sendMessage(String SQSUrl, String msg) {
        SQS.sendMessage(SendMessageRequest.builder()
                .queueUrl(SQSUrl)
                .messageBody(msg)
                .delaySeconds(10)
                .build());
    }

    public String getSQSUrl(String queueName) {
        while (true) {
            try {
                GetQueueUrlResponse getQueueUrlResponse = SQS.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
                String queueUrl = getQueueUrlResponse.queueUrl();
                return queueUrl;
            } catch (QueueDoesNotExistException e) {
                continue;
            } catch (SqsException e) {
                System.err.println(e.awsErrorDetails().errorMessage());
                System.exit(1);
            }
        }
    }

    public void deleteMessageFromSQS(String queueUrl, Message message) {
        try {
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
            SQS.deleteMessage(deleteMessageRequest);

        } catch (AwsServiceException e) {
            e.printStackTrace();
        }
    }

    public String downloadFileFromS3(String bucketName, String keyName) {
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .build();
            ResponseBytes<GetObjectResponse> res = S3.getObjectAsBytes(getObjectRequest);
            return res.asUtf8String();
        } catch (AwsServiceException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<Message> pullNewMessages(String queueUrl) {
        try {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .visibilityTimeout(100)
                    .build();
            return SQS.receiveMessage(receiveMessageRequest).messages();
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
    }


    public void shutdownInstance() {
        TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                .instanceIds(EC2MetadataUtils.getInstanceId())
                .build();
        EC2.terminateInstances(request);
    }

    public void shutdownInstanceByID(String instanceId) {
        TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();
        EC2.terminateInstances(request);
    }

    public Message getNewMessage(String queueUrl) {
        try {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .visibilityTimeout(100) //TODO - to check if it is enough
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(20)
                    .build();
            List<Message> messages = SQS.receiveMessage(receiveMessageRequest).messages();
            if (messages.size() > 0)
                return messages.get(0);
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
    }

    public boolean checkIfKeyAlreadyExist(String bucketName, String keyName) {
        return new AmazonS3Client().doesObjectExist(bucketName, keyName);
    }

    public void deleteSQS(String queueName) {
        try {
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();

            String queueUrl = SQS.getQueueUrl(getQueueRequest).queueUrl();
            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build();

            SQS.deleteQueue(deleteQueueRequest);

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public void deleteBucket(String bucketName) {
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.DEFAULT_REGION).build();
        try {
            System.out.println(" - removing objects from bucket");
            ObjectListing object_listing = s3.listObjects(bucketName);
            while (true) {
                for (Iterator<?> iterator =
                     object_listing.getObjectSummaries().iterator();
                     iterator.hasNext(); ) {
                    S3ObjectSummary summary = (S3ObjectSummary) iterator.next();
                    s3.deleteObject(bucketName, summary.getKey());
                }
                // more object_listing to retrieve?
                if (object_listing.isTruncated()) {
                    object_listing = s3.listNextBatchOfObjects(object_listing);
                } else {
                    break;
                }
            }
            VersionListing version_listing = s3.listVersions(
                    new ListVersionsRequest().withBucketName(bucketName));
            while (true) {
                for (Iterator<?> iterator =
                     version_listing.getVersionSummaries().iterator();
                     iterator.hasNext(); ) {
                    S3VersionSummary vs = (S3VersionSummary) iterator.next();
                    s3.deleteVersion(
                            bucketName, vs.getKey(), vs.getVersionId());
                }

                if (version_listing.isTruncated()) {
                    version_listing = s3.listNextBatchOfVersions(
                            version_listing);
                } else {
                    break;
                }
            }

            System.out.println(" OK, bucket ready to delete!");
            s3.deleteBucket(bucketName);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
        System.out.println("Deleted bucket " + bucketName);
    }

    public void changeMessageVisibility(String SQSUrl, String receipt) {
        SQS.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                .queueUrl(SQSUrl)
                .visibilityTimeout(100)
                .receiptHandle(receipt)
                .build());
    }
}