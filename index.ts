import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

const name = "wired-hasura-cache";
const stack = pulumi.getStack();

const tags = {
  Name: `${name}-${stack}`,
  Project: "Wired.network",
  PulumiStack: `Pulumi-${stack}`,
};

const config = new pulumi.Config();
const maintainer = config.require("maintainer");

const networkingStackRef = new pulumi.StackReference(
  `${maintainer}/relation-networking/prod`
);

const securityGroup = new aws.ec2.SecurityGroup(tags.Name, {
  vpcId: networkingStackRef.getOutput("dataVpcId"),
  ingress: [
    {
      fromPort: 6379,
      toPort: 6379,
      protocol: "tcp",
      cidrBlocks: [
        networkingStackRef.getOutput("eksVpcCidrBlock"),
      ],
    },
  ],
});

const wiredAwsEcrStackRef = new pulumi.StackReference(`${maintainer}/wired-aws-ecr/prod`)

const hasuraEngineStack = new pulumi.StackReference(
  `${maintainer}/relation-hasura-engine-eks/${stack}`
);

const redisConfig = {
  nodeType: "cache.t3.small",
  numCacheClusters: 2,
};

if (stack === "prod") {
  redisConfig.nodeType = "cache.r6g.xlarge";
  redisConfig.numCacheClusters = 4;
}

const cacheClusterSubnets = new aws.elasticache.SubnetGroup(tags.Name, {
  subnetIds: networkingStackRef.getOutput("dataVpcPrivateSubnetIds"),
  tags,
});

const redisReplicationGroup = new aws.elasticache.ReplicationGroup(
  tags.Name,
  {
    engine: "redis",
    applyImmediately: true,
    automaticFailoverEnabled: true,
    nodeType: redisConfig.nodeType,
    parameterGroupName: "default.redis7.cluster.on",
    numCacheClusters: redisConfig.numCacheClusters,
    port: 6379,
    subnetGroupName: cacheClusterSubnets.name,
    securityGroupIds: [securityGroup.id],
    description: "hasura cache's redis replication group",
    tags,
  },
);

export const ecrRepositoryName = name;
export const ecrRepositoryUrl = wiredAwsEcrStackRef
  .getOutput("ecrRepositories")
  .apply((repos) => repos[name]);

export const hasuraEngineSecretId = hasuraEngineStack
  .getOutput("awsSecretId");

export const redisClusterEndpoint = redisReplicationGroup.configurationEndpointAddress;
