import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

const stack = pulumi.getStack();
const name = "wired-hasura-cache";

const baseTags = {
  Name: `${name}-${stack}`,
  Project: "Wired.network",
  PulumiStack: `Pulumi-${stack}`,
};

const config = new pulumi.Config();
const maintainer = config.require("maintainer");

const networkingStack = new pulumi.StackReference(
  `${maintainer}/relation-networking/dev`
);

const peeredSecurityGroup = aws.ec2.SecurityGroup.get(
  "eks-vpc-data-vpc-sg",
  networkingStack.getOutput("peeredSecurityGroupId")
);

const wiredAwsEcrStackRef = aws
  .getRegionOutput()
  .apply(
    (res) =>
      new pulumi.StackReference(`${maintainer}/wired-aws-ecr/${res.name}`)
  );

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

const cacheClusterSubnets = new aws.elasticache.SubnetGroup(baseTags.Name, {
  subnetIds: networkingStack.getOutput("dataVpcPrivateSubnetIds"),
  tags: baseTags,
});

const redisReplicationGroup = new aws.elasticache.ReplicationGroup(
  baseTags.Name,
  {
    engine: "redis",
    applyImmediately: true,
    automaticFailoverEnabled: true,
    nodeType: redisConfig.nodeType,
    parameterGroupName: "default.redis7.cluster.on",
    numCacheClusters: redisConfig.numCacheClusters,
    port: 6379,
    subnetGroupName: cacheClusterSubnets.name,
    securityGroupIds: [peeredSecurityGroup.id],
    description: "hasura cache's redis replication group",
  }
);

export const ecrRepositoryName = name;
export const ecrRepositoryUrl = wiredAwsEcrStackRef.apply((stack) =>
  stack.getOutput("ecrRepositories").apply((repos) => repos[name])
);
export const hasuraEngineSecretId = hasuraEngineStack.getOutput(
  "hasuraEngineSecretId"
);
export const redisClusterEndpoint =
  redisReplicationGroup.configurationEndpointAddress;
