import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

const stack = pulumi.getStack();

const baseName = "hasura-cache";

const baseTags = {
  Name: `${baseName}-${stack}`,
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

const hasuraEngineStack = new pulumi.StackReference(
  `${maintainer}/relation-hasura-engine-eks/${stack}`
);

let ecrRepository;

const redisConfig = {
  nodeType: "cache.t3.medium",
  numCacheClusters: 2,
};

if (stack === "prod") {
  redisConfig.nodeType = "cache.m6g.2xlarge";
  redisConfig.numCacheClusters = 4;

  ecrRepository = new aws.ecr.Repository(baseName, {
    name: baseName,
    tags: baseTags,
  });

  new aws.ecr.RepositoryPolicy(
    baseName,
    {
      repository: ecrRepository.id,
      policy: JSON.stringify({
        Version: "2012-10-17",
        Statement: [
          {
            Sid: "new policy",
            Effect: "Allow",
            Principal: "*",
            Action: [
              "ecr:GetDownloadUrlForLayer",
              "ecr:BatchGetImage",
              "ecr:BatchCheckLayerAvailability",
              "ecr:PutImage",
              "ecr:InitiateLayerUpload",
              "ecr:UploadLayerPart",
              "ecr:CompleteLayerUpload",
              "ecr:DescribeRepositories",
              "ecr:GetRepositoryPolicy",
              "ecr:ListImages",
              "ecr:DeleteRepository",
              "ecr:BatchDeleteImage",
              "ecr:SetRepositoryPolicy",
              "ecr:DeleteRepositoryPolicy",
            ],
          },
        ],
      }),
    },
    { deleteBeforeReplace: true }
  );

  new aws.ecr.LifecyclePolicy(
    baseName,
    {
      repository: ecrRepository.id,
      policy: JSON.stringify({
        rules: [
          {
            rulePriority: 1,
            description: "Expire images older than 14 days",
            selection: {
              tagStatus: "untagged",
              countType: "sinceImagePushed",
              countUnit: "days",
              countNumber: 14,
            },
            action: {
              type: "expire",
            },
          },
        ],
      }),
    },
    { deleteBeforeReplace: true }
  );
} else {
  ecrRepository = aws.ecr.Repository.get(baseName, baseName);
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
    parameterGroupName: "default.redis6.x.cluster.on",
    numCacheClusters: redisConfig.numCacheClusters,
    port: 6379,
    subnetGroupName: cacheClusterSubnets.name,
    securityGroupIds: [peeredSecurityGroup.id],
    description: "hasura cache's redis replication group",
  }
);

export const ecrRepositoryName = ecrRepository.name;
export const ecrRepositoryUrl = ecrRepository.repositoryUrl;
export const hasuraEngineSecretId = hasuraEngineStack.getOutput(
  "hasuraEngineSecretId"
);
export const redisClusterEndpoint =
  redisReplicationGroup.configurationEndpointAddress;
