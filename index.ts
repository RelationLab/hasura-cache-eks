import * as aws from "@pulumi/aws";
import * as pulumi from "@pulumi/pulumi";
import * as k8s from "@pulumi/kubernetes";

const name = "wired-hasura-cache";
const stack = pulumi.getStack();

const tags = {
  Name: `${name}-${stack}`,
  Project: "Wired.network",
  PulumiStack: `Pulumi-${stack}`,
  "map-migrated-cnmap": "d-server-00j4w9vvvwd0hj",
  "map-migrated-project-id": "GCR-Migration-2021-531",
};

const config = new pulumi.Config();
const organization = pulumi.getOrganization();

const networkingStackRef = new pulumi.StackReference(
  `${organization}/relation-networking/prod`
);

const securityGroup = new aws.ec2.SecurityGroup(tags.Name, {
  vpcId: networkingStackRef.getOutput("dataVpcId"),
  ingress: [
    {
      fromPort: 6379,
      toPort: 6379,
      protocol: "tcp",
      cidrBlocks: [networkingStackRef.getOutput("eksVpcCidrBlock")],
    },
  ],
});

const wiredAwsEcrStackRef = new pulumi.StackReference(
  `${organization}/wired-aws-ecr/prod`
);

const wiredEksStackRef = new pulumi.StackReference(
  `${organization}/relation-eks/prod`
);

const hasuraEngineStack = new pulumi.StackReference(
  `${organization}/relation-hasura-engine-eks/${stack}`
);

const redisConfig = {
  numNodeGroups: 1,
  clusterMode: false,
  nodeType: "cache.t3.small",
};

if (stack === "prod") {
  redisConfig.clusterMode = true;
  redisConfig.numNodeGroups = 2;
  redisConfig.nodeType = "cache.r6g.xlarge";
}

const redisAuthToken = config.requireSecret("redisAuthToken");

const kmsKey = new aws.kms.Key(`${tags.Name}-kms-key`, {
  keyUsage: "ENCRYPT_DECRYPT",
  description: `kms key for security ${tags.Name} redis auth token`,
  tags,
});

const encryptedRedisAuthToken = new aws.kms.Ciphertext(
  `${tags.Name}-encrypted-redis-auth-token`,
  {
    keyId: kmsKey.id,
    plaintext: redisAuthToken,
  }
);

const serviceAccountName = `${name}-sa`;

const assumeRolePolicy = aws.iam.getPolicyDocumentOutput({
  version: "2012-10-17",
  statements: [
    {
      actions: ["sts:AssumeRoleWithWebIdentity"],
      conditions: [
        {
          test: "StringEquals",
          values: ["sts.amazonaws.com"],
          variable: pulumi.interpolate`${wiredEksStackRef.getOutput(
            "eksClusterOidcProviderURL"
          )}:aud`,
        },
        {
          test: "StringEquals",
          values: [
            pulumi.interpolate`system:serviceaccount:${stack}:${serviceAccountName}`,
          ],
          variable: pulumi.interpolate`${wiredEksStackRef.getOutput(
            "eksClusterOidcProviderURL"
          )}:sub`,
        },
      ],
      effect: "Allow",
      principals: [
        {
          type: "Federated",
          identifiers: [
            wiredEksStackRef.getOutput("eksClusterOidcProviderARN"),
          ],
        },
      ],
    },
  ],
});

const rolePolicy = new aws.iam.Policy(`${tags.Name}-role-policy`, {
  policy: {
    Version: "2012-10-17",
    Statement: [
      {
        Action: "kms:Decrypt",
        Effect: "Allow",
        Resource: kmsKey.arn,
      },
    ],
  },
});

const iamRole = new aws.iam.Role(`${tags.Name}-iam-role`, {
  assumeRolePolicy: assumeRolePolicy.apply((policy) => policy.json),
  inlinePolicies: [
    {
      name: `${tags.Name}-inline-policy`,
      policy: rolePolicy.policy,
    },
  ],
});

new k8s.core.v1.ServiceAccount(serviceAccountName, {
  metadata: {
    name: serviceAccountName,
    namespace: stack,
    annotations: {
      "eks.amazonaws.com/role-arn": iamRole.arn,
    },
  },
});

const cacheClusterSubnets = new aws.elasticache.SubnetGroup(
  `${tags.Name}-subnet-group`,
  {
    subnetIds: networkingStackRef.getOutput("dataVpcPrivateSubnetIds"),
    tags,
  }
);

const redisReplicationGroup = new aws.elasticache.ReplicationGroup(
  `${tags.Name}-redis`,
  {
    port: 6379,
    engine: "redis",
    engineVersion: "7.0",
    applyImmediately: true,
    authToken: redisAuthToken,
    nodeType: redisConfig.nodeType,
    transitEncryptionEnabled: true,
    securityGroupIds: [securityGroup.id],
    multiAzEnabled: redisConfig.clusterMode,
    subnetGroupName: cacheClusterSubnets.name,
    numNodeGroups: redisConfig.clusterMode ? 2 : 1,
    automaticFailoverEnabled: redisConfig.clusterMode,
    replicasPerNodeGroup: redisConfig.clusterMode ? 2 : 0,
    parameterGroupName: redisConfig.clusterMode
      ? "default.redis7.cluster.on"
      : "default.redis7",
    description: `hasura cache's redis replication group for ${stack}`,
    tags,
  }
);

export const ecrRepositoryName = name;
export const ecrRepositoryUrl = wiredAwsEcrStackRef
  .getOutput("ecrRepositories")
  .apply((repos) => repos[name]);

export const hasuraEngineSecretId = hasuraEngineStack.getOutput("awsSecretId");

export const kmsEncryptedRedisAuthToken =
  encryptedRedisAuthToken.ciphertextBlob;

export const redisEndpoint = redisReplicationGroup.clusterEnabled.apply(
  (cluster) =>
    cluster
      ? redisReplicationGroup.configurationEndpointAddress
      : redisReplicationGroup.primaryEndpointAddress
);
