import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import org.apache.commons.logging.impl.Log4JLogger;
import py4j.GatewayServer;

import java.sql.Time;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.logging.Log;
/**
 * Class as EC2 java server, which will serve py4j client call
 * and call EC2 java client to act on AWS EC2 service.
 */
public class EC2JavaServer {
    private static AmazonEC2 ec2 = null;
    private static AvailabilityZone availabilityZone = null;

    static {
        System.setProperty("aws.access",
                "AKIAJTG3W4YWID4RGFNQ");
        System.setProperty("aws.secret",
                "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");
    }

    public EC2JavaServer() {
        if (ec2 != null) {
            return;
        }

        // AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
        AWSCredentials credentials = new BasicAWSCredentials(
                System.getProperty("aws.access"),
                System.getProperty("aws.secret"));
        ec2 = new AmazonEC2Client(credentials);
        ec2.setRegion(Region.getRegion(Regions.AP_SOUTHEAST_1));
    }

    private static AvailabilityZone getAvailZone() throws Exception {
        if (availabilityZone == null) {
            DescribeAvailabilityZonesResult result =
                    ec2.describeAvailabilityZones();
            if (!result.getAvailabilityZones().isEmpty()) {
                availabilityZone = result.getAvailabilityZones().get(0);
                System.out.print("available zone:" + availabilityZone);
            } else {
                throw new Exception("No availability zones");
            }
        }

        return availabilityZone;
    }

    /**
     * Create EC2 volume from EC2 snapshot
     * @param OpenstackSnapshotId note, this snapshotId is from openstack
     *                            side, which we need map it to EC2 snapshot
     *                            id
     * @return volumeId in EC2
     * @throws Exception
     */
    public String createVolumeFromSnapshot(String OpenstackSnapshotId)
            throws Exception {
        DescribeSnapshotsRequest describeSnapshotsRequest =
                new DescribeSnapshotsRequest().withFilters(new Filter().
                        withName("description").
                        withValues("*" + OpenstackSnapshotId + "*"));
        DescribeSnapshotsResult describeSnapshotsResult =
                ec2.describeSnapshots(describeSnapshotsRequest);
        if (describeSnapshotsResult.getSnapshots().size() >= 1) {
            for (Snapshot snapshot : describeSnapshotsResult.getSnapshots()) {
                System.out.print("snapshot:" + snapshot.getSnapshotId());
            }
            throw new Exception("snapshot is not unique or empty");
        }

        String EC2SnapshotId =
        		describeSnapshotsResult.getSnapshots().get(0).getSnapshotId();
        CreateVolumeRequest request = new CreateVolumeRequest();
        request.setSnapshotId(EC2SnapshotId);
        request.setAvailabilityZone(getAvailZone().getZoneName());
        CreateVolumeResult result = ec2.createVolume(request);
        return result.getVolume().getVolumeId();
    }

    /**
     * launch one EC2 instance from specified AMI id.
     * Note, we don't use key-pair to login, which requires the image is
     * created with default user
     * @param EC2ImageId AMI image id
     * @param name instance name as tag
     * @return {"public-ip":, "instance-id":}
     */
    public HashMap<String, String> launchInstanceFromAMI(
            String EC2ImageId, String name) throws Exception {
        // run instance according to image
        Image img = ec2.describeImages(new DescribeImagesRequest().
                withImageIds(EC2ImageId)).getImages().get(0);
        InstanceType type = (img.getVirtualizationType().equals("hvm")) ?
                InstanceType.T2Micro:InstanceType.T1Micro;
        RunInstancesRequest req = new RunInstancesRequest(EC2ImageId, 1, 1).
                withInstanceType(type).withPlacement(
                new Placement().withAvailabilityZone(getAvailZone().getZoneName()));
        List<Instance> insts = ec2.runInstances(req).getReservation().getInstances();

        // tag it with name
        Instance instance = insts.get(0);
        ec2.createTags(new CreateTagsRequest().withResources(
                instance.getInstanceId()).withTags(new Tag("Name", name)));

        // wait at most 3 minutes until instance running
        int timeOut = 180;
        DescribeInstanceStatusRequest describeInstanceStatusRequest =
                new DescribeInstanceStatusRequest().withInstanceIds(
                        instance.getInstanceId());
        while (timeOut > 0) {
            DescribeInstanceStatusResult status =
                    ec2.describeInstanceStatus(describeInstanceStatusRequest);
            if (!status.getInstanceStatuses().isEmpty() &&
                    status.getInstanceStatuses().get(0).getInstanceState().
                    getName().equals("running")) {
                break;
            } else {
                Thread.sleep(10*1000);
            }
            timeOut -= 10;
        }

        if (timeOut <= 0) {
            throw new Exception("timed out to wait vm to be running");
        }

        // get public ip
        DescribeInstancesRequest describeInstancesRequest =
                new DescribeInstancesRequest().withInstanceIds(
                        instance.getInstanceId());
        DescribeInstancesResult instancesResult =
                ec2.describeInstances(describeInstancesRequest);
        // fill result
        HashMap<String, String> result = new HashMap<String, String>();
        result.put("public-ip", instancesResult.getReservations().get(0).
                getInstances().get(0).getPublicIpAddress());
        result.put("instance-id", instance.getInstanceId());
        return result;
    }

    /**
     * attach volume to instance, with specified device name
     * @param EC2volumeId
     * @param EC2InstanceId
     * @param mountPoint
     * @return
     */
    public String attachVolumeToInstance(
            String EC2volumeId, String EC2InstanceId, String mountPoint) {
        AttachVolumeResult result = ec2.attachVolume(
                new AttachVolumeRequest().withVolumeId(EC2volumeId).
                        withInstanceId(EC2InstanceId).withDevice(mountPoint));
        return result.getAttachment().getDevice();
    }

    public static void main(String [] args) {
        // TODO: make the port configurable
        GatewayServer gatewayServer = new GatewayServer(new EC2JavaServer(), 25535);
        gatewayServer.start();
        /*EC2JavaServer ec2JavaServer = new EC2JavaServer();
        try {
            HashMap<String, String> result =
                    ec2JavaServer.launchInstanceFromAMI("ami-79e8c42b", "test3");
            System.out.println("publicIp:" + result.get("public-ip"));
            System.out.println("instance:" + result.get("instance-id"));
        } catch (Exception e) {
            e.printStackTrace();
        }*/
    }
}