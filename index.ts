import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

const stack = pulumi.getStack();

const baseTags = {
    Name: `hasura-cache-${stack}`,
    Project: "Relation",
    PulumiStack: `Pulumi-${pulumi.getStack()}`
};

const config = new pulumi.Config();
const hasuraEngineServiceName = config.require("hasuraEngineServiceName");
const hasuraEngineServicePort = config.get("hasuraEngineServicePort") || 8080;
const maintainer = config.require("maintainer");

const networkingStack = new pulumi.StackReference(`${maintainer}/relation-networking/${stack}`);
const peeredSecurityGroup = aws.ec2.SecurityGroup.get("eks-vpc-data-vpc-sg", networkingStack.getOutput("peeredSecurityGroupId"));

const hasuraEngineStack = new pulumi.StackReference(`${maintainer}/relation-hasura-engine-eks/${stack}`);

const ecrRepository = new aws.ecr.Repository(baseTags.Name, {tags: baseTags});

new aws.ecr.RepositoryPolicy(baseTags.Name, {
    repository: ecrRepository.id,
    policy: JSON.stringify({
        Version: "2012-10-17",
        Statement: [{
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
                "ecr:DeleteRepositoryPolicy"
            ]
        }]
    }),
}, {deleteBeforeReplace: true});

new aws.ecr.LifecyclePolicy(baseTags.Name, {
    repository: ecrRepository.id,
    policy: JSON.stringify({
        rules: [{
            rulePriority: 1,
            description: "Expire images older than 14 days",
            selection: {
                tagStatus: "untagged",
                countType: "sinceImagePushed",
                countUnit: "days",
                countNumber: 14
            },
            action: {
                type: "expire"
            }
        }]
    })
}, {deleteBeforeReplace: true});


const cacheClusterSubnets = new aws.elasticache.SubnetGroup(baseTags.Name, {
    subnetIds: networkingStack.getOutput("dataVpcPrivateSubnetIds"),
    tags: baseTags,
});


const replicationGroup = new aws.elasticache.ReplicationGroup(baseTags.Name, {
    engine: "redis",
    automaticFailoverEnabled: true,
    nodeType: "cache.m6g.large",
    parameterGroupName: "default.redis6.x.cluster.on",
    numCacheClusters: 3,
    port: 6379,
    subnetGroupName: cacheClusterSubnets.id,
    securityGroupIds: [peeredSecurityGroup.id],
    description: "hasura cache's redis replication group",
});


export const ecrRepositoryInfo = {
    name: ecrRepository.name,
    url: ecrRepository.repositoryUrl,
};

export const hasuraEngine = {
    secretId: hasuraEngineStack.getOutput("hasuraEngineSecretId"),
    serviceName: hasuraEngineServiceName,
    servicePort: hasuraEngineServicePort,
};

export const redisClusterEndpoint = replicationGroup.configurationEndpointAddress;
