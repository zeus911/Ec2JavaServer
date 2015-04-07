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
import java.util.Collections;
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
     * @param openstackSnapshotId note, this snapshotId is from openstack
     *                            side, which we need map it to EC2 snapshot
     *                            id
     * @param name
     * @return volumeId in EC2
     * @throws Exception
     */
    public String createVolumeFromSnapshot(String openstackSnapshotId, String name)
            throws Exception {
        DescribeSnapshotsRequest describeSnapshotsRequest =
                new DescribeSnapshotsRequest().withFilters(new Filter().
                        withName("description").
                        withValues("*" + openstackSnapshotId + "*"));
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

        // tag it with name
        ec2.createTags(new CreateTagsRequest().withResources(
                result.getVolume().getVolumeId()).withTags(
                new Tag("Name", name)));
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
     * Delete EC2 instance by EC2 instanceId
     * @param instanceId
     * @throws Exception
     */
    public void deleteInstance(String instanceId) throws Exception {
        TerminateInstancesRequest request = new TerminateInstancesRequest().withInstanceIds(
                Collections.singletonList(instanceId));
        TerminateInstancesResult result = ec2.terminateInstances(request);
        if (result.getTerminatingInstances().isEmpty()) {
            throw new Exception("failed to delete Instance");
        }
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

    /**
     * Create volume from blank with specified name and size
     * @param name name as tag which will be used to anchor the volume
     * @param size
     * @return
     * @throws Exception
     */
    public String createVolume(String name, int size) throws Exception {
        CreateVolumeRequest request = new CreateVolumeRequest().
                withAvailabilityZone(getAvailZone().getZoneName()).
                withSize(size);
        CreateVolumeResult result = ec2.createVolume(request);

        // tag it with name
        ec2.createTags(new CreateTagsRequest().withResources(
                result.getVolume().getVolumeId()).withTags(
                new Tag("Name", name)));
        return result.getVolume().getVolumeId();
    }

    /**
     * get EC2 instance Id from name tag
     * @param name
     * @return
     */
    public String getInstanceIdFromName(String name) throws Exception {
        String result = "None";
        DescribeInstancesRequest describeInstancesRequest =
                new DescribeInstancesRequest().withFilters(new Filter().
                        withName("tag:Name").withValues(name));

        DescribeInstancesResult describeInstancesResult = ec2.describeInstances(
                describeInstancesRequest);
        if (!describeInstancesResult.getReservations().isEmpty() &&
                !describeInstancesResult.getReservations().get(0).getInstances().isEmpty()) {
            result = describeInstancesResult.getReservations().get(0).
                    getInstances().get(0).getInstanceId();
        }
        return result;
    }

    /**
     * Get volume ec2 Id from name tag
     * @param name
     * @return
     * @throws Exception
     */
    public String getVolumeIdFromName(String name) throws Exception {
        String result = "None";
        DescribeVolumesRequest describeVolumesRequest =
                new DescribeVolumesRequest().withFilters(new Filter().
                        withName("tag:Name").withValues(name));
        DescribeVolumesResult describeVolumesResult = ec2.describeVolumes(
                describeVolumesRequest);
        if (!describeVolumesResult.getVolumes().isEmpty()) {
            result = describeVolumesResult.getVolumes().get(0).getVolumeId();
        }

        return result;
    }

    /**
     * get instance status from instanceId
     * @param instanceId
     * @return
     */
    public String getInstanceStatus(String instanceId) {
        String result = "None";
        DescribeInstancesRequest request = new DescribeInstancesRequest().
                withInstanceIds(Collections.singletonList(instanceId));
        DescribeInstancesResult describeInstancesResult
                = ec2.describeInstances(request);
        System.out.println("instances:" + result);
        if (!describeInstancesResult.getReservations().isEmpty() &&
                !describeInstancesResult.getReservations().get(0).getInstances().isEmpty()) {
            result = describeInstancesResult.getReservations().get(0).
                    getInstances().get(0).getState().getName();
        }
        return result;
    }

    public static void testDeleteAllInstances() {
        EC2JavaServer ec2JavaServer = new EC2JavaServer();
        try {
            DescribeInstancesRequest request = new DescribeInstancesRequest();
            DescribeInstancesResult result = ec2.describeInstances(request);
            for (Reservation reservation : result.getReservations()) {
                for(Instance instance : reservation.getInstances()) {
                    ec2JavaServer.deleteInstance(instance.getInstanceId());
                }
            }
            System.out.println("result:" + result);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void testGetNetwork()  {
        EC2JavaServer ec2JavaServer = new EC2JavaServer();
        DescribeInstancesRequest request = new DescribeInstancesRequest();
        DescribeInstancesResult result = ec2.describeInstances(request);
        System.out.println("instances:" + result);

        for (Reservation reservation : result.getReservations()) {
            for(Instance instance : reservation.getInstances()) {
                if (!instance.getNetworkInterfaces().isEmpty()) {
                    String networkId = instance.getNetworkInterfaces().get(0).getNetworkInterfaceId();
                    DescribeNetworkInterfacesRequest describeNetworkInterfacesRequest =
                            new DescribeNetworkInterfacesRequest().
                                    withNetworkInterfaceIds(Collections.singletonList(networkId));
                    DescribeNetworkInterfacesResult result1 =
                            ec2.describeNetworkInterfaces(describeNetworkInterfacesRequest);
                    System.out.println("network:" + result1);
                }
            }
        }
    }

    public static void testGetInstance(String name) {
        EC2JavaServer ec2JavaServer = new EC2JavaServer();
        try {
            ec2JavaServer.launchInstanceFromAMI("ami-79e8c42b", name);
        } catch (Exception e) {
            e.printStackTrace();
        }

        DescribeInstancesRequest request = new DescribeInstancesRequest();
        DescribeInstancesResult result = ec2.describeInstances(request);
        System.out.println("instances:" + result);

        try {
           String id = ec2JavaServer.getInstanceIdFromName(name);
            System.out.println("id:" + id);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void testGetInstanceStatus(String name) {
        EC2JavaServer ec2JavaServer = new EC2JavaServer();
        try {
            ec2JavaServer.launchInstanceFromAMI("ami-79e8c42b", name);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            String id = ec2JavaServer.getInstanceIdFromName(name);
            System.out.println("id:" + id);
            String status = ec2JavaServer.getInstanceStatus(id);
            System.out.println("status:" + status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String [] args) {
        // TODO: make the port configurable
        GatewayServer gatewayServer = new GatewayServer(new EC2JavaServer(), 25535);
        gatewayServer.start();
    }
}
